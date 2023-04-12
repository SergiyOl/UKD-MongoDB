const fetch = require("node-fetch");
const zlib = require("zlib");
const readline = require("readline")
const { MongoClient } = require("mongodb");
const { connected } = require("process");

const url = "https://popwatch-staging.s3.us-east-2.amazonaws.com/movies_1.gz";
const dbUrl = "mongodb+srv://serhiioleniak:LWSvTd@mycluster.cyt8h4g.mongodb.net/?retryWrites=true&w=majority";
const dbName = "moviesDB";
const collectionName = "movies";

const client = new MongoClient(dbUrl);

fetch(url)
  .then((res) => {
    const gunzip = zlib.createGunzip();
    const stream = res.body.pipe(gunzip);
    const rl = readline.createInterface({ input: stream });

    const movies = [];

    rl.on("line", (line) => {
        const movie = JSON.parse(line);
        movies.push(movie);
        console.log(movie);
    });

      rl.on("close", () => {
        client.connect((err) => {
        if (err) throw err;
        const db = client.db(dbName);
        const collection = db.collection(collectionName);

        collection
          .insertMany(movies)
          .then((res) => {
            console.log(
              `Inserted ${movies.length} documents into ${collectionName}`
            );
            client.close();
          })
          .catch((err) => {
            console.error(err);
            client.close();
          });
      });
    });
  })
  .catch((err) => console.error(err));