Twilytic (Server)
============

Communicates with client and database.

## Usage

Install [RethinkDB](https://www.rethinkdb.com/docs/install/), [Nodejs](https://nodejs.org/en/) and [npm](https://www.npmjs.com/get-npm)

Start the RethinkDB server:

```bash
rethinkdb
```

Clone the repository, install dependencies and run the server. On the first run, if not existing, the database `twitterdb` will be created with the tables `tweets` and `users`.

```bash
git clone https://github.com/art1fa/twilytic-server.git
cd twilytic-server
npm install
npm start
```
## Note 

To make Twilytic work, you also need to install, configure and run [twilytic-client](https://github.com/art1fa/twilytic-client) and [twilytic-scripts](https://github.com/art1fa/twilytic-scripts).


## About

Twilytic is the outcome of my Master's thesis at the Technical University of Munich. The thesis was issued and supervised by Prof. Dr. Jürgen Pfeffer from the chair of Computational Social Science and Big Data. Thank you so much!