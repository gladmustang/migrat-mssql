var pkg = require('./package.json');
var fs = require('fs');
var parser = require('./lib/parser.js');
var sql = require('mssql');


module.exports = function(options) {
	options.port = options.port || 1433;
	options.host = options.host || 'localhost';

    const config = {
        user: options.user,
        password: options.password,
        server: options.host,
        database: options.database,

        options: {
            encrypt: options.encrypt
        }
    }


   return function(migrat) {

       var client;
	   var migratTable = options.migratSchema + '.' + options.migratTable;

       	function createClient(callback) {
                var pool = new sql.ConnectionPool(config, err => {
                    if (err) err = new Error('Unable to connect to SQL Server (message: "' + (err.message || err) + '")');
                    callback(err, pool);

            })
		}

		function queryExecutor(sql) {
			if (!sql) return null;
			return function(context, callback) {
				client.request().query(sql, function(err, result) {
					if (err) return callback(new Error('SQL Server query failed: ' + (err.message || err)));
					callback();
				});
			};
		}

        function checkExecutor(sql) {
			if (!sql) return null;
			return function(context, callback) {
				client.request().query(sql, function(err, result) {
					if (err) return callback(err);
					if (!result.recordset.length) {
						return callback(new Error('SQL Server check failed (query returned zero rows)'));
					}
					callback();
				});
			};
		}

        function setValue(key, value, callback) {
			var escaped_key = '\''+key+'\'';
			var escaped_value = '\''+value+'\'';
			var sql = [
				'UPDATE ' + migratTable + ' SET value=' + escaped_value + ' WHERE migratkey=' + escaped_key,
				'INSERT INTO ' + migratTable + ' (migratkey, value) SELECT ' + escaped_key + ', ' + escaped_value + ' WHERE NOT EXISTS (SELECT 1 FROM ' + migratTable + ' WHERE migratkey=' + escaped_key + ' AND value like' + escaped_value + ')'
			].join(';');
			console.log(sql);
			client.request().query(sql, function(err, result) {
				if (err) return callback('Unable to set SQL Server value (message: "' + (err.message || err) + '")');
				callback();
			});
		}

        migrat.setPluginName('migrat-mssql');
        migrat.setPluginVersion(pkg.version);

        migrat.registerHook('initialize', function(callback) {
			createClient(function(err, pool) {
				if (err) {
                    pool.close();
					return callback(err);
				}

				client = pool;

				client.request().query('select * from information_schema.schemata where schema_name=\''+options.migratSchema+'\'', function(err, result){
					if(err) return callback(new Error('Unable to get SQL Server schema info: ' + options.migratSchema + ' (message: "' + (err.message || err) + '")'));
					if(result.recordset.length<=0) {
						client.request().query('CREATE SCHEMA ' + options.migratSchema, function(err) {
							if (err) return callback(new Error('Unable to create SQL Server schema: ' + migratTable + ' (message: "' + (err.message || err) + '")'));
						});
					}

					client.request().query('select * from information_schema.tables where table_schema=\''+options.migratSchema+'\' and table_name='+ '\''+ options.migratTable+ '\'', function(err, result){
						if(err) return callback(new Error('Unable to get SQL Server table info: ' + migratTable + ' (message: "' + (err.message || err) + '")'));
						if(result.recordset.length>0) {
							callback();
						} else {
							client.request().query('CREATE TABLE ' + migratTable + ' (migratkey varchar(22) PRIMARY KEY, value text)', function(err) {
								if (err) return callback(new Error('Unable to create SQL Server table: ' + migratTable + ' (message: "' + (err.message || err) + '")'));
								callback();
							});								
						}
					});
				});

				// client.request().query('CREATE SCHEMA IF NOT EXISTS ' + options.migratSchema, function(err) {
				// 	if (err) return callback(new Error('Unable to create SQL Server schema: ' + options.migratSchema + ' (message: "' + (err.message || err) + '")'));
				// 	client.request().query('CREATE TABLE IF NOT EXISTS ' + migratTable + ' (key varchar(22) PRIMARY KEY, value text)', function(err) {
				// 		if (err) return callback(new Error('Unable to create SQL Server table: ' + migratTable + ' (message: "' + (err.message || err) + '")'));
				// 		callback();
				// 	});
				// });
			});
		});

        migrat.registerHook('terminate', function(callback) {
			if (client) client.close();
			callback();
		});

        migrat.registerLoader('*.mssql', function(file, callback) {
			fs.readFile(file, 'utf8', function(err, source) {
				if (err) return callback(err);
				parser(source, function(err, queries) {
					//console.log(queries.up);
					if (err) return callback(err);
					callback(null, {
						up: queryExecutor(queries.up),
						down: queryExecutor(queries.down),
						check: checkExecutor(queries.check)
					});
				});
			});
		});


        migrat.registerTemplate('mssql', function(details, callback) {
			fs.readFile(__dirname + '/lib/template.mssql', 'utf8', function(err, source) {
				if (err) return callback(err);
				callback(null, source
					.replace('{{date}}', (new Date(details.timestamp)).toString())
					.replace('{{attribution}}', details.user ? ' by ' + details.user : '')
				);
			});
		});

		if (options.enableLocking) {
			migrat.registerLocker({
				lock: function(callback) {
					function attemptLock() {
						createLock(function(err, acquired) {
							if (err) return callback(err);
							if (acquired === true) return callback();
							setTimeout(attemptLock, 500);
						});
					}

					function createLock(callback) {
						var rollback = function(client) {
							client.request().query('ROLLBACK', function(err) {
								return client.close();
							});
						};
						createClient(function(err, pool) {
							pool.request().query('BEGIN', function(err) {
								if (err) {
									rollback(pool);
									return callback(new Error('Failed to begin transaction'));
								}
								pool.request().query('SELECT * FROM ' + migratTable + ' WHERE migratkey = \'lock\'', function(err, result) {
									if (err) {
										rollback(pool);
										return callback(err);
									}
									if (result.recordset.length) {
										rollback(pool);
										return callback(null, false);
									}
									pool.request().query('INSERT INTO ' + migratTable + ' (migratkey, value) VALUES ($1, $2)', ['lock', String((new Date()).getTime())], function(err) {
										if (err) {
											rollback(pool);
											return callback(err);
										}
										pool.query('COMMIT', function(err) {
											pool.close();
											if (err) {
												callback(err);
											} else {
												callback(null, true);
											}
										});
									});
								});
							});
						});
					}

					attemptLock();
				},
				unlock: function(callback) {
					client.query('DELETE FROM ' + migratTable + ' WHERE migratkey=\'lock\'', callback);
				}
			});
		}

		if (options.enableStateStorage) {
			migrat.registerGlobalStateStore({
				get: function(callback) {
					client.request().query('SELECT * FROM ' + migratTable + ' WHERE migratkey = \'state\'', function(err, result) {
						if (err) return callback(err);
						if (!result.recordset.length) return callback();
						callback(null, result.recordset[0].value);
					});
				},
				set: function(state, callback) {
					setValue('state', state, callback);
				}
			});
		}
       
   };    
};