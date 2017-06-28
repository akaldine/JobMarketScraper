'use strict';

var co = require('co');

var parser = require('./parser');
(async function () {
    try {

        var MongoClient = require('mongodb').MongoClient, assert = require('assert');
        var dbUrl = 'mongodb://localhost:27017/myproject';
        var timestamp = Date.now();

        var db = await MongoClient.connect(dbUrl);
        var stats = await parser.parse(db)
        //console.log(stats);

        //console.log(timestamp);
        var promises = [];
        await stats.forEach((element) => {
            promises.push((async function() {
                element.timestamp = timestamp;
                await db.collection('stats').insert(element)
                .then(()=> console.log('done with stats'));
            }));
        });
        console.log("awaiting db closing");
        await Promise.all(promises);
        console.log("closing db");
        //db.close();

    }
    catch (err) {
        console.error("Runner Failiure : " + err)
    };
    //console.log(stats);

})();