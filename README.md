# migrat-mssql

migrat-config.js

var mssql = require('migrat-mssql');
```
module.exports = {
    plugins: [
        mssql({
            host: 'hostname',
            port: port number,
            user: 'user name',
            password: 'password',
            database: 'database name',
            migratSchema: 'achema name',
            migratTable: 'table name',
            enableLocking: false,
            enableStateStorage: true,
            encrypt: false, // Use this if you're on Windows Azure 
        })
    ],
	  migrationsDir: './migrations', 
    localState: './mig_version'

};

```
