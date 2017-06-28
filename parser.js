'use strict';
var co = require('co');
var exports = module.exports = {};
var categories = require('./categories').categories;
//console.log(categories);
var stats = [];

//var stats = new Array(categories.length);

module.exports.parse =
    async function (db) {
        console.log("test");

        var promises = [];

        categories.forEach(async function (category) {
            console.log("new promise created for processing category " + category.type);
            promises.push(parseCategory(db, category, promises));
            console.log("done with promise for category" + category.type);
        });        
        await Promise.all(promises);
        console.log("returning from parser");
        return stats;
    }

async function parseCategory(db, category, promises) {
    try {
        let primaryDB = db.collection('documents');
        let refinedDB = db.collection('refine');
        // Get the documents collection
        let counters = new Object();
        counters.numMonthly = 0;
        counters.numAnually = 0;
        counters.numNoPeriod = 0;
        counters.totalrefined = 0;
        counters.havenoSalary = 0;
        counters.total = 0;

        //console.log("here");
        //console.log(refinedDB);

        await refinedDB.remove();
        //console.log("here1");
        // Find some documents
        var cursor = primaryDB.find({ type: category.type }, { salary: 1, title: 1, link: 1, site: 1, type: 1, timestamp: 1 });
        //console.log("here2");
        while (await cursor.hasNext()) {
            //console.log("here3");
            counters.total++;
            var doc = await cursor.next();
            //console.log("here4");
            let promise = await processDoc(doc, refinedDB, counters);
            promise ? promises.push(promise) : "";

        }
        console.log("here5");
        stats.push({
            totalrefined: counters.totalrefined,
            havenoSalary: counters.havenoSalary,
            numMonthly: counters.numMonthly,
            numAnually: counters.numAnually,
            numNoPeriod: counters.numNoPeriod,
            total: counters.total,
            site: category.site,
            type: category.type
        });

    }
    catch (err) {
        console.log(err);
    }
}

async function processDoc(doc, refinedDB, counters) {
    let promise;
    doc.newsalary = doc.salary;
    doc.salary = doc.salary.replace(/ /g, '');
    doc.salary = doc.salary.replace(/,/g, '');

    var nums = doc.salary.match(/\d+/g);

    if (doc.salary && nums != null) {
        var salnum = getSalaryNumber(nums, doc.salary);
        doc.period = getPeriod(doc.salary);
        switch (doc.period) {
            case 'annually':
                counters.numMonthly++;
                break;
            case 'monthly':
                counters.numAnually++;
                break;
            default:
                counters.numNoPeriod++;
        }
        doc.num = salnum;
        counters.totalrefined++;

        delete doc._id;        
        promise =refinedDB.insert(doc);
        console.log("done with doc" + doc.type);            
        

    }
    else counters.havenoSalary++;
    return promise;
}





//helpers
function getSalaryNumber(nums, salary) {
    var n1 = nums[0];
    var i1 = salary.indexOf(n1);
    var l1 = n1.length;
    var n = n1;

    var n2, i2, l2;

    if (nums.length > 1) {
        var n2 = nums[1];
        var i2 = salary.indexOf(n2);
        var l2 = n2.length;
        n = n2;
    }
    if ((salary[i1 + l1] || '').toLowerCase() == 'k' ||
        (salary[i2 + l2] || '').toLowerCase() == 'k')
        return parseFloat(n * 1000);
    else return parseFloat(n);
}
function getPeriod(str) {
    //annual
    var astrings = ['pa', 'p\/a', 'p\\a', 'p\.a', 'annu', 'annum'];
    var asearch = new RegExp(astrings.join('|'), 'i');
    var a = str.search(asearch);

    if (a != -1)
        return 'annually';

    astrings = ['pm', 'p\/m', 'p\\m', 'p\.m', 'mon'];
    asearch = new RegExp(astrings.join('|'), 'i');
    a = str.search(asearch);

    if (a != -1)
        return 'monthly';
    else return '';
}