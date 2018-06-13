
module.exports = {
  database: {
    host: "localhost",
    port: 28015,
    db: "twitterdb"
  }
}

// FOR PRODUCTION DATABASE
// var fs = require('fs');

// var caCert = fs.readFileSync('certificate.cert');

// module.exports = {
//   database: {
//     host: "xyz.com",
//     port: 28000,
//     db: "twitterdb",
//     user:'user',
//     password: 'pw',
//     ssl: {
//       ca: caCert,
//     }
//   }
// };