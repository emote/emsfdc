// Start a proxy standalone with:
// echo "{}" | node startProxy.js
// Then run this test script:
// node standaloneFairyland.js http://localhost:9000 creds.json 

var request = require('request');
var util = require('util');
var fs = require('fs');
var async = require('async');
// var session = require('mmscli').session;

var session = { directive: directive };

var proxyURL = process.argv[2];
var credentials = JSON.parse(fs.readFileSync(process.argv[3]));
console.dir(credentials);

var uniqueName = 'Mark' + new Date().toString();

selectAll();

function directive(body,callback) {
    var cdmOptions = new Object();
    cdmOptions['credentials'] = credentials;          

    body.options = cdmOptions;

    var requestBody = JSON.stringify(body);

    var requestOptions = {
        uri: proxyURL,
        method: 'POST',
        json: body,
    };

    request(requestOptions, function (error, response, body) {
        if(error) {
            console.log(util.inspect(error,false,null));
            console.log("Terminate with error.");
            process.exit(1);
        }
        if(body.status==='ERROR') {
            console.log(util.inspect(body,false,null));
            console.log("Terminate with error.");
            process.exit(1);
        }
        console.log(body);
        callback(error,body);
    });   
}

function selectAll(err,res) {
    session.directive(
        {op: "SELECT", targetType:"Fairyland__c", properties:["id","number_of_guests__c","name_of_visitor__c","time_of_visit__c"]}
    ,deleteResultList);
}

function deleteResultList(err,result) {
    console.dir(result);
    if(result.results.length > 0) {
        console.log('Found ' + result.results.length + ' rows in Fairyland__c');
        async.forEachSeries(result.results,
            function(row,cb) {
                var request = {op: "DELETE",targetType: "Fairyland__c",where: {externalId: row.externalId}};
                console.dir(request);
                session.directive(request,cb);
            },
        insert1);
    } else {
        insert1();
    }
}

function insert1() {
    console.log("Inserting, name is " + uniqueName);
    session.directive({
        op:'INSERT',
        targetType: 'Fairyland__c', 
        values: {
            time_of_visit__c: '2011-10-31',
            name_of_visitor__c: uniqueName,
            number_of_guests__c: '2',
        }
    },wait1);
}

function wait1(err,res) {
    console.log('waiting...');
    setTimeout(select1,2000);
}

function select1(res) {
    session.directive({
        op: 'SELECT',
        targetType: 'Fairyland__c',
        properties:["id","number_of_guests__c","name_of_visitor__c","time_of_visit__c"], 
        where: {name_of_visitor__c:uniqueName}
    },uploadImage);
}

var resourceName;
var mongoId;

function uploadImage(err,res) {
    console.dir(res);

    mongoId = res.results[0].externalId;
    if(!mongoId) {
        console.log('FAILURE: Object was not created in Salesforce!');
        return;
    }

    var data = fs.readFileSync('icon.gif');
    var folderId = 'photos';
    var documentName = 'pic' + new Date().getTime();
    
    resourceName = '/' + folderId + '/' + documentName;

    session.directive({
        op: 'INVOKE',
        targetType: 'Document',
        name: 'refresh',
        params: {
            name: resourceName,
            folderId: 'photos',
            contentType: 'image/gif',
            content: data.toString('base64')
        }         
    },update1);

    console.log("Uploading image: " + resourceName);
}

function update1(err,res) {
    console.log("Upload complete");
     console.log("Updating...");

    session.directive({
        op:'UPDATE',
        targetType: 'Fairyland__c', 
        values: {
            photo_name__c: resourceName,
        },
        where : {externalId: mongoId}
    },download);
 }

function download(err,res) {
    console.log('downloading ' + uniqueName);
    session.directive({
        op: 'SELECT',
        targetType: 'Fairyland__c',
        properties:["id","number_of_guests__c","name_of_visitor__c","time_of_visit__c","photo_name__c"],
        where: {name_of_visitor__c:uniqueName}
    },getImage);
}

function getImage(err,res) {
    console.dir(res)

    var resourcePath = res.results[0].photo_name__c;

    console.log('Resource to download = ' + resourcePath);

    var part = resourcePath.split('/');

    session.directive({
        op: 'INVOKE',
        targetType: 'Document',
        name: 'fetch',
        params: {
            name: resourcePath
        }         
    },done);

}

function done(err,res) {
    console.dir(res);
    console.log('Done!');
    console.log('unique name = ' + uniqueName);
}

