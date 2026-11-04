// Cube configuration options: https://cube.dev/docs/config
/** @type{ import('@cubejs-backend/server-core').CreateOptions } */
// cube.js
const MssqlDriver = require("@cubejs-backend/mssql-driver");

module.exports = {
  driverFactory: ({ dataSource }) => new MssqlDriver({
    server: process.env.CUBEJS_DB_HOST,
    database: process.env.CUBEJS_DB_NAME,
    user: process.env.CUBEJS_DB_USER,
    password: process.env.CUBEJS_DB_PASS,
    port: Number(process.env.CUBEJS_DB_PORT),
    options: {
      encrypt: false,              // disable TLS for local dev
      trustServerCertificate: true, // if you do want encryption, trust the self-signed cert instead
      connectTimeout: 30000,
    },
  }),
};