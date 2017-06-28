'use strict';
let async = require('async');
let request = require('request-promise-native');
let cheerio = require('cheerio');
let MongoClient = require('mongodb').MongoClient, assert = require('assert');
let co = require('co');
let parser = require('./parser');
let categories = require('./categories').categories;

let dbUrl = 'mongodb://localhost:27017/myproject';

let timestamp = Date.now();
connect();
let reconnectAttempt=0;

async function connect() {
    MongoClient.connect(dbUrl, {
        reconnectTries: 60,
        reconnectInterval: 1000,
        autoReconnect: true,
    }).then((db) =>
            main(db)
        ).catch(() =>{
            console.log("Connect Failed : Reconnect Attempt " + ++reconnectAttempt );
            setTimeout(connect, 1000);                        
        });
        
}

async function main(db) {
    try {

        let promises = [];
        console.log("Retrieving");
        let noDocsRemoved = await db.collection('documents').remove();


        for (let k = 0; k < categories.length; k++) {
            //await processCategory(categories, k, db);
            console.log("category " + k)
            promises.push(processCategory(categories, k, db));
        }

        await Promise.all(promises);

        console.log("done with first section, move on to parsing");

        let stats = await parser.parse(db);

        console.log(stats);
        console.log("Writing Stats");
        async.each(stats, (element, err) => {
            promises.push(new Promise((resolve, reject) => {
                //console.log(element);
                element.timestamp = timestamp;
                db.collection('stats')
                    .insert(element)
                    .then(() => {
                        //console.log("inserted into stats");
                        resolve();
                    });
            }));
        });

        await Promise.all(promises);

        console.log("done with second section,closing db");
        db.close();

        console.log("done");
    }
    catch (err) {
        console.log(err);
    }
}
async function processCategory(category, k, db) {
    try {
        let promises = [];
        let newurl = categories[k].url;
        let next;
        let pageNum = 0;
        //console.log("In Category " + categories[k].type)
        do {
            console.log("requesting  category: " + categories[k].type + " page no. " + pageNum);
            //console.log(newurl);
            let html = await request(newurl);
            console.log("request done for category " + categories[k].type + " page no. " + pageNum)
            let $ = cheerio.load(html);
            //console.log("request succesfull for category: " + categories[k].type + "page no. " +pageNum);
            next = $("a.btn:contains('next')");
            // //console.log(next.eq(0).attr("href") + "within " + categories[k].type);
            newurl = "https://www.pnet.co.za" + next.eq(0).attr("href");

            var cheerios = $("ol").eq(0).children();

            async.eachOfLimit(cheerios, 1, async function (el, ind) {
                //console.log("parsing box " + ind)
                let json = await getJson(categories, k, el, ind, $);
                //console.log("done parsing box " + ind)
                promises.push(new Promise((resolve, reject) => {
                    db.collection('documents').insert(json)
                        .then(() => {
                            ////console.log(json);
                            ////console.log("inserted into " + categories[k].site + "page " + pageNum);
                            resolve();
                        });
                }));
            });
            pageNum++;

            ////console.log("moving to page " + pageNum);
        } while (next.length > 0)
        await Promise.all(promises);
    }
    catch (err) {
        //console.error(err); 
    }
}



async function getJson(categories, k, el, ind, $) {

    let json = new Object();
    json.title = $(el).eq(0).find("h5 span").text();
    json.salary = $(el).eq(0).find("dd[data-offer-meta-salary='']").text();
    json.description = $(el).eq(0).find("span[itemprop='description']").text();


    json.link = $(el).eq(0).find("a").eq(1).attr("href");
    json.time = $(el).eq(0).find("time").attr("datetime");
    json.location = $(el).eq(0).find("span[itemprop='addressLocality']").html();
    json.organization = $(el).eq(0).find("div[itemprop='hiringOrganization'] span").html();
    json.statute = $(el).eq(0).find("dd[data-offer-meta-statute='']").html();
    // //console.log("awaiting timeout box " + k)
    // await new Promise(resolve => setTimeout(resolve, 10000));;
    // //console.log("done awaiting box " + k);

    //json.fulldescription = await getfulldescription(json.link);

    json.site = categories[k].site;
    json.type = categories[k].type;
    json.timestamp = timestamp;

    return json;
}

async function getfulldescription(link) {
    try {
        let html = await request(link);
    }
    catch (err) {
        //console.log(err); 
    }
    let $ = cheerio.load(html);
    let json = new Object();
    json.intro = $("#company-intro").text();
    json.tasks = $("#job-tasks").text();
    json.requim = $("#job-requim").text();

    return json;

}











