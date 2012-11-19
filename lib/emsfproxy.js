var sfdcSess = require('./sfdcSess');
var soql = require('./soql');
var util = require('util');

var verbose;
var targetTypes;

// Validates that the request contains credentials, 
// establishes a Salesforce session with the credential,
// passes control to processOperation
exports.processDirective = function processDirective(requestData,callback) {
    var restResult;

    var options = requestData.options;
    if(!(options && options.credentials)) {
        // An error on validate credentials returns a normal restResponse
        callback(null,{
            targetType: 'RestResult',
            status: 'ERROR',
            errors: [{
                targetType:'CdmError',
                errorCode:'integration.login.fail.nocredentials',
                errorMessage:"No credentials have been entered"
            }]
        }); 
    } else if(requestData.op === 'INVOKE' && requestData.targetType === "CdmExternalCredentials") {
        // Only this operation does not require a session
        sfdcSess.loginWithAPIToken(options.credentials,function(err,serverUrl,sessionId) {
            if(err) {
                callback(null,{
                    targetType: 'RestResult',
                    status: 'ERROR',
                    errors: [{
                        targetType:'CdmError',
                        errorCode: (err.errorCode.indexOf('integration.login.fail')===0 ? err.errorCode : 'integration.login.fail'),
                        errorMessage:err.message
                    }]
                });
            } else {
                callback(null,{
                    targetType: 'RestResult',
                    status: 'SUCCESS'
                });
            }
        });
    } else {
//console.log('here it is');
        // Every other operation requires a session and metadata for the targetType
        sfdcSess.getSession(options.credentials,function(err,session) {
            if(err) return callback(err);
            // sfdcSess.describeSObject(session,requestData.targetType,function(err,metadata) {}
//console.log('logged in, getting metadata...');
            session.getMetadataForTargetType(requestData.targetType,function(err,typeInfo) {
                if(err) return callback(err);
//console.log('got metadata, sending query');
                processOperation(session,typeInfo,requestData,function(err,responseData) {
                    callback(err,responseData);
                });
            });
        });
    }
}

function processOperation(session,typeInfo,directive,callback) {
    // callback should return (err,responseData)

    var errList = [];
    var method;
    var expectedStatus;
    var resource;

    if(directive.op === 'INVOKE' && directive.targetType === "Document") {
        //console.log("Directive for INVOKE on Document: " + util.inspect(directive,true,null));
        if(!directive.params || !directive.params.name) {
            return callback(new Error("Document operation must include a name param"));   
        }
        if(directive.name == "fetch") {
            return session.fetchDocument(directive,callback);
        } else if(directive.name == "refresh") {
            return session.upsertDocument(directive,callback);
        } else {
            return callback(new Error("Unsupported INVOKE operation: " + directive.name)); 
        }
    }

    // Filter out field names from values and property lists that do not exist in the list of field from
    // the Salesforce metadata. This is because every Salesforce user may see a different set of fields based
    // on their permissions. Asking for a field you can't see gives a "400 Malformed query" error. In our
    // proxies we prefer to just return null values for fields that are not visible.

    if(directive.values) {
        directive.values = filterValuesByMetadata(directive.values,typeInfo.fields);
    }

    if(directive.properties) {
        directive.properties = directive.properties.filter(function(item) {
            if(typeof typeInfo.fields[item] === 'undefined') {
                console.log('Ignoring request for a property for a field which is undefined for current user. Field name=' + item);
                return false;
            } else {
                return true;
            }
        });
    }

    var fetchDeletesSince = 0;

    if(directive.op === 'SELECT') {
        method = 'GET';
        expectedStatus = 200;

        if(directive.options.cursorId) {
            resource = directive.options.cursorId;
            console.log('Fetching additional rows for: ' + directive.targetType + ' with ' + resource);
        } else {
            var columns = combineColumns(directive.properties,["Id","SystemModstamp"]);
            var query = "SELECT " + columns.join(',') + " from "+directive.targetType;
            if(directive.where) {
                // Salesforce has a rule that replication data is not kept for more than 30 days
                // so if there is a condition on SystemModStamp to fetch data from more that 30 days
                // ago, then we will just refetch the entire table
                if(directive.where.SystemModstamp) {
                    fetchDeletesSince = directive.where.SystemModstamp['$gt'];
                    var elapsed = (new Date()).getTime() - fetchDeletesSince;
                    if(elapsed > (30 * 24 * 60 * 60 * 1000)) { // days * hours * minutes * seconds * ms
                        delete directive.where.SystemModstamp;
                        fetchDeletesSince = 0;
                    }
                }
                var soqlString = soql.mongoQuerytoSOQL(directive.where,typeInfo);
                if(soqlString) {
                    query += ' WHERE ' + soqlString;
                }
            }
            if(verbose) console.log('Sending SOQL query: ' + query);
            resource = 'query/?q=' + encodeURIComponent(query);
        }       
    } else if(directive.op === 'INSERT') {
        method = 'POST';
        expectedStatus = 201;
        resource = 'sobjects/'+directive.targetType+'/';
    } else if(directive.op === 'UPDATE') {
        method = 'PATCH';
        expectedStatus = 204;
        resource = 'sobjects/'+directive.targetType+'/'+directive.where.externalId;
    } else if(directive.op === 'DELETE') {
        method = 'DELETE';
        expectedStatus = 204;
        resource = 'sobjects/'+directive.targetType+'/'+directive.where.externalId;
    } else {
        callback(new Error('Unrecognized REST operation in request: ' + util.inspect(restRequest, true, null)));
        return;
    }

    if(errList.length > 0) {
        callback(errList);
        return;
    }

    session.restRequest(method,resource,directive.values,function(err,result) {
        if(err) return callback(err);

        if(result.status != expectedStatus) {
            return callback(new Error('Unsuccessful status code from ' + method + ': ' + result.status + ", resource = " + resource));
        }

        restResult = {
            targetType: 'RestResult',
            status: 'SUCCESS',
            externalId: result.id
        }

        if(method==='GET') {

//console.log('WHOLE RESULTS ARE')
//console.log(util.inspect(restResult.results,false,null));

// console.log(util.inspect(result,false,null));

            restResult.cursor = result.jsonBody.nextRecordsUrl;
            restResult.count = result.jsonBody.totalSize;
            restResult.results = convertToCdmResult(errList,result.jsonBody,typeInfo);
            if(restResult.cursor) {
                console.log('Query returned ' 
                + restResult.results.length + ' ' 
                + directive.targetType 
                + ' and a cursor: ' 
                + restResult.cursor);
            }
        }

        if(fetchDeletesSince > 0) {
            // SFDC has an undocumented rule that startDate must be at least one minute less than endDate
            var now = new Date();
            if(now.getTime() - fetchDeletesSince < 60001) {
                fetchDeletesSince = now.getTime() - 60001;
            }

            session.getDeleted(directive.targetType,
                soql.ISODateString(new Date(fetchDeletesSince)),
                soql.ISODateString(now),function(err,result) {
                    
                if(err) return callback(err);
                if(result.deletedRecords) {
                    //console.log('Deleted records provided by Salesforce:');
                    //console.dir(result.deletedRecords);
                    addDeletesToCdmResult(errList,result.deletedRecords,restResult.results);
                }
                callback(null,restResult);
            });
        } else {
            callback(null,restResult);
        }
    });
}

function filterValuesByMetadata(values,metadata) {
    var retval = {};
    for(var key in values) {
        if(typeof metadata[key] === 'undefined') {
            console.log('Ignoring update value for a field which is undefined for current user. Field name=' + key);
        } else if(metadata[key].updateable) {
            retval[key] = values[key];
        } else {
            console.log('Ignoring update value for a non-updateable field, Field name=' + key);            
        }
    }
    return retval;
}

function combineColumns(a,b) {
    var map = {};
    map = addToMap(map,a);
    map = addToMap(map,b);
    return mapToStrArr(map);
}

// Array of strings to map of keys with value true
function addToMap(map,arr) {
    var retval = {};
    for(var i=0;i<arr.length;i++) {
        map[arr[i]] = true;
    }
    return map;    
}

// map of keys with value true to Array of strings
function mapToStrArr(map) {
    var retval = [];
    for(var key in map) {
        retval.push(key);
    }
    return retval;    
}

function convertToCdmResult(errList,result, mappedType) {
    var retval = [];
    if(result.records) {
        for (var i=0; i < result.records.length; i++) {
            var record = result.records[i];
            var cdmRecord = convertOutputValues(errList,record, mappedType);
            retval.push(cdmRecord);
        }
    }
    return retval;
}

function addDeletesToCdmResult(errList,deletedRecords,cdmResults) {
    // SFDC REST API JSON follows a convention of returning either an Array or a single object
    if(deletedRecords) {
        if(deletedRecords instanceof Array) {
            for (var i=0; i < deletedRecords.length; i++) {
                var record = deletedRecords[i];
                addDeleteToCdmResult(record,cdmResults);
            }
        } else {
            addDeleteToCdmResult(deletedRecords,cdmResults);
        }
    }
    return cdmResults;
}

function convertOutputValues(errList,values, typeInfo) {
    var sfData = new Object();
    for(var key in values) {
        if(key === 'attributes') {
           // skip over Salesforce attributes
//console.log('skip over Salesforce attributes');
//console.dir(values); 
        } else if(key === 'Id') {
            sfData.externalId = values[key];
        } else if(key === 'SystemModstamp') {   
            sfData.externalTimestamp = new Date(values[key]).getTime();
        } else if(typeInfo.fields[key].type === 'datetime') {
            sfData[key] = new Date(values[key]).getTime();
        } else {
            sfData[key] = values[key];
        }
    }
    return sfData;
}

function addDeleteToCdmResult(record,cdmResults) {
    var cdmRecord = new Object();
    cdmRecord.externalId = record.id;
    cdmRecord.cdmDeleted = true;
    cdmResults.push(cdmRecord);
}
