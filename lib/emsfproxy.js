"use strict";

var sfdcSess = require('./sfdcSess');
var soql = require('./soql');
var util = require('util');
var emutils = require('emutils');
var async = require('async');

var verbose;
var defaultColumnsArr = ["Id","SystemModstamp","CreatedDate"];
var defaultColumnsMap = addToMap({},defaultColumnsArr);

emutils.announceSelf(__dirname);

exports.sfdcSess = sfdcSess; // export this for use in emote deploy command

var salesforceTypeMap = {
    'STRING':'String',
    'BOOLEAN':'Boolean',
    'INT':'Integer',
    'DOUBLE':'Real',
    'DATE':'DateString',
    'DATETIME':'Date',
    'BASE64':'String',
    'ID':'String',
    'REFERENCE':'String',
    'CURRENCY':'Dollars',
    'TEXTAREA':'String',
    'PERCENT':'Percent',
    'PHONE':'String',
    'URL':'String',
    'EMAIL':'String',
    'COMBOBOX':'String',
    'PICKLIST':'String',
    'MULTIPICKLIST':'String',
    'ANYTYPE':'String'
};

exports.setInitialConfig = function setInitialConfig(initialConfig) {
    // TODO: this is a temporary fix pending a change in the MMS server
    // to provide the endpoint in the request credentials
    sfdcSess.settings.verbose = initialConfig.verbose;
    verbose = initialConfig.verbose;
    sfdcSess.settings.sfVersion = initialConfig.sfVersion;
}

// Validates that the request contains credentials,
// establishes a Salesforce session with the credential,
// passes control to processOperation
exports.processDirective = function processDirective(requestData,callback) {
    var restResult;

    var options = requestData.options;
    if(!(options && options.credentials)) {
        // An error on validate credentials returns a normal restResponse
        callback(null,{
            targetType: 'RestResponse',
            status: 'ERROR',
            errors: [{
                targetType:'CdmError',
                code:'integration.login.fail.nocredentials',
                message:"No credentials have been entered"
            }]
        });
    } else if (requestData.targetType == "CdmExternalSystem") {
        var found = false;
        if (requestData.op == "INVOKE") {
            if (requestData.name == "getSalesforceTypes") {
                found = true;
                sfdcSess.getSession(options.credentials,function(err,session) {
                    if(err) return callback(err);
                    if (session.errors && session.errors.length > 0) {
                        return callback(null, session);
                    }
                    session.sfVersion = requestData.params.sfVersion;
                    return session.getSalesforceTypes(callback);
                });
            }
            else if (requestData.name == "createSfModel") {
                found = true;
                sfdcSess.getSession(options.credentials,function(err,session) {
                    if(err) return callback(err);
                    if (session.errors && session.errors.length > 0) {
                        return callback(null, session);
                    }
                    session.sfVersion = requestData.params.sfVersion;
                    return createSfModel(session, requestData.params.typeDescs,requestData.params.externalSystem,callback);
                });
            }
        }
        if (!found) {
            return callback(new Error("Unsupported request type."));
        }
    }
    else
    {
        // Every other operation requires a session and metadata for the targetType
        sfdcSess.getSession(options.credentials,function(err,session) {
            if(err) return callback(err);
            if (session.errors && session.errors.length > 0) {
                return callback(null, session);
            }
            session.getMetadataForTargetType(requestData.targetType,function(err,typeInfo) {
                if(err) return callback(err);
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

    if(directive.properties) {
        directive.properties = combineColumns(directive.properties,defaultColumnsArr);
        directive.properties = directive.properties.filter(function(item) {
            if(typeof typeInfo.fields[item] === 'undefined') {
                if(!defaultColumnsMap[item]) {
                    console.log('Ignoring request for a property for a field which is undefined for current user. Field name=' + item);
                }
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
            if(!directive.properties) {
                return callback(new Error('SELECT request must contain a property list'));
            }
            var query = "SELECT " + directive.properties.join(',') + " from "+directive.targetType;
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
        directive.values = filterValuesByMetadata(directive.values,typeInfo.fields,'createable');
    } else if(directive.op === 'UPDATE') {
        method = 'PATCH';
        expectedStatus = 204;
        resource = 'sobjects/'+directive.targetType+'/'+directive.where.externalId;
        directive.values = filterValuesByMetadata(directive.values,typeInfo.fields,'updateable');
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

            // Build error from Salesforce error repsonse, e.g.
            // [{"fields":[],
            // "message":"Your attempt to delete Mr. Josh Davis could not be completed because it is associated with the following cases.: 00001005\n",
            // "code":"DELETE_FAILED"}]

            var message = 'Unexpected status code from ' + method + ': ' + result.status + ", resource = " + resource;

            err = new Error(message);

            if(result.jsonBody) {
                // console.log('Salesforce error: ' + util.inspect(result.jsonBody,true,null));
                var sfError = result.jsonBody[0];
                if(sfError) {
                    message
                    err.code = 'integration.Salesforce.' + sfError.code;
                    err.message = 'Salesforce error: ' + sfError.message;
                    err.fields = sfError.fields;
                }
            }

            return callback(err);
        }

        var restResult = {
            targetType: 'RestResponse',
            status: 'SUCCESS',
            externalId: (result.jsonBody ? result.jsonBody.id : null)
        }

        if(verbose && restResult.externalId) {
            console.log('Returning Salesforce generated id = ' + restResult.externalId);
        }

        if(method==='GET') {
            restResult.cursor = result.jsonBody.nextRecordsUrl;
            restResult.count = result.jsonBody.totalSize;
            if(verbose) console.log('Salesforce query returned ' + restResult.count + ' rows.');
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

function filterValuesByMetadata(values,metadata,filter) {
    var retval = {};
    for(var key in values) {
        if(typeof metadata[key] === 'undefined') {
            console.log('Ignoring value for a field which is undefined for current user. Field name=' + key);
        } else if(metadata[key][filter]) {
            retval[key] = values[key];
        } else {
            console.log('Ignoring value that is not ' + filter + ', Field name=' + key);
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

    // Some Salesforce objects have no SystemModstamp since they are not updateable,
    // set externalTimestamp to CreateDate
    if(!sfData.externalTimestamp) {
        sfData.externalTimestamp = new Date(values['CreatedDate']).getTime();
    }

    return sfData;
}

function addDeleteToCdmResult(record,cdmResults) {
    var cdmRecord = new Object();
    cdmRecord.externalId = record.id;
    cdmRecord.cdmDeleted = true;
    cdmResults.push(cdmRecord);
}

function createSfModel(session, typeDescs, externalSystem, callback) {
    var warnings = [];
    var model = [];

    session.getSalesforceTypes(function (err, result) {
        if (err) {
            return callback(err);
        }
        var allSfTypes = {};
        var sfTypeArray = result.results;
        sfTypeArray.forEach(function(row) {
            allSfTypes[row] = true;
        });

        var typeArr = [];
        delete typeDescs._verbose_logging_;
        for (var name in typeDescs) {
            if (!allSfTypes[name]) {
                addWarning('WARNING: type ' + name + ' was not found in your Salesforce metadata.' +
                    ' It will not be included in you generated CDM types.', 'integration.type.not,found');
                continue;
            }
            var td = typeDescs[name];
            td.name = name;
            typeArr.push(td);
        }
        async.forEachSeries(typeArr, createType, function(err, result) {
            if (err) {
                return calback(err);
            }
            return callback(null,
                {
                    targetType: 'RestResponse',
                    status: 'SUCCESS',
                    results: model,
                    warnings: warnings
                });
        });
    });

    function createType(typeDesc, cb) {
        session.describeSObject(typeDesc.name, function(err, sfDesc) {
            if (err) {
                return cb(err);
            }

            var propList = [];
            var targetProperties = [];
            var pickListTypes = [];

            var attributes = sfDesc.fields;
            var attrMap = {};
            for (var attrName in attributes) {
                var attr = attributes[attrName];
                attrMap[attr.name] = true;

                if(!typeDesc.properties
                    || typeDesc.properties[attr.name]
                    || attr.name == "Id"
                    || attr.name == "SystemModstamp"
                    || attr.name == "CreatedDate"
                    || attr.name == "CreatedById")
                {
                    // if a property list is supplied, check to see if this property occurs in the list

                    var cdmType;
                    var propertyInfo = typeDesc.properties[attr.name];
                    if(propertyInfo && propertyInfo.type) {
                        cdmType = propertyInfo.type;
                    } else {
                        cdmType = salesforceTypeMap[attr.type.toUpperCase()];
                    }
                    if(!cdmType) cdmType = 'String';

                    if(attr.picklistValues && attr.picklistValues.length > 0) {
                        // If this is a picklist, then we create a cdmType to hold the enumeration

                        var enumurationTypeName = typeDesc.target + '_enum_' + attr.name;
                        var enumeration = [];
                        attr.picklistValues.forEach(function(row) {
                            enumeration.push({label:row.label,value:row.value});
                        });

                        pickListTypes.push({enumurationTypeName:enumurationTypeName,enumeration:enumeration});

                        if (attr.type == "picklist")
                        {
                            cdmType = enumurationTypeName;
                        }
                    }

                    if (attr.name != "Id")
                    {
                        propList.push({name: attr.name, type: cdmType});
                    }

                    targetProperties.push({name: attr.name, type: cdmType, externalType: attr.type});
                }
            }

            if(typeDesc.properties) {
                for(var propName in typeDesc.properties) {
                    if(!attrMap[propName]) {
                        addWarning('WARNING: property ' + propName + ' was not found in your Salesforce metadata.' +
                            ' It will not be included in the generated CDM type: ' + typeDesc.target);
                    }
                }
            }

            for (var item in pickListTypes) {
                replaceEnumeration(item.enumurationTypeName,item.enumeration);
            }
            processSalesforceTypeContinued(typeDesc, externalSystem, propList,targetProperties, cb);
        })
    }

    function replaceEnumeration(typeName,enumeration) {

        model.push({
            op: 'INVOKE',
            targetType: 'CdmType',
            name: 'alterCdmType',
            params: {
                replace: true,
                typeName: typeName,
                storage: 'scalar',
                scalarBaseType: 'String',
                scalarInheritsFrom : 'String',
                isEnumerated: 'true',
                isScalar: true,
                overrideAllowed: true,
                enumeration: enumeration,
                indices: []
            }
        });
    }

    function processSalesforceTypeContinued(typeDesc, externalSystem, propList, targetProperties, cb) {

        var  myTypeName = typeDesc.target;
        model.push({
            op: 'INVOKE',
            targetType: 'CdmType',
            name: 'alterCdmType',
            params: {
                replace: true,
                typeName: myTypeName,
                storage: 'document',
                extensionAllowed: true,
                externallySourced: true,
                propertySet: propList,
                indices: [[
                    {property: 'externalId', order: 1},
                    {options: {unique: false}}
                ]]
            }
        });
        createBinding(myTypeName,typeDesc.name, typeDesc.typeBindingProperties, targetProperties, externalSystem);

        if(myTypeName === 'Opportunity') {
            // Default SFDC Opportunity object requires special handling of enumerated StageName:

            addEnumeration('Opportunity_enum_StageName','OpportunityStage',
                {label:'MasterLabel',value:'MasterLabel',extraData:{
                    DefaultProbability:'DefaultProbability',
                    IsClosed:'IsClosed',
                    IsWon:'IsWon',
                    IsActive:'IsActive',
                    SortOrder:'SortOrder'}
                }, cb);

        }
        else if(myTypeName === 'Task') {
            addEnumeration('Task_enum_Status','TaskStatus',
                {label:'MasterLabel',value:'MasterLabel',
                    extraData:{IsClosed:'IsClosed'}
                }, cb);
        }
        else {
            cb(null);
        }
    }

    function createBinding(cdmName, sfdcName, bindingProperties, propList, externalSystem) {

        model.push({
            op: 'INVOKE',
            targetType: 'CdmSimpleSchemaOperations',
            name: 'deleteTargetDefs',
            params: {
                cdmType: cdmName
            }
        });
        model.push({
            op: 'INVOKE',
            name : "create",
            targetType: 'CdmTargetType',
            params: {
                targetType : {
                    name: sfdcName,
                    externalSystem: externalSystem,
                    properties: propList
                }
            }
        });
        var typeBinding = {
            name: cdmName,
            externalType: sfdcName,
            externalSchema: externalSystem,
            readStrategy: 'sync',
            readPeriod: null,
            sourceStrategy: 'sync',
            writeStrategy: 'sync',
            uniqueExternalId: true
        };
        if (bindingProperties) {
            emutils.merge(bindingProperties, typeBinding, true);
        }

        // first determine if this is a field that does not have a SystemModstamp,
        // (e.g. OpportunityFieldHistory) -- if there is not SystemModstamp, then
        // the CDM externalTimestamp field must map to the SFDC CreatedDate field
        var hasSystemModstamp = false;
        for(var j=0;j<propList.length;j++) {
            var bindProp = propList[j];
            if(bindProp.name === 'SystemModstamp') {
                hasSystemModstamp = true;
                break;
            }
        }

        // propList is: [{name: attr.name, type: cdmType, externalType: attr.type},...]
        var propertyBindingList = [];

        for(var j=0;j<propList.length;j++) {
            var bindProp = propList[j];

            var cdmProp = bindProp.name; // default to SFDC name
            var propType = bindProp.externalType;
            if(bindProp.name === 'Id') {
                cdmProp = 'externalId';
                propType = 'string';
            } else if(bindProp.name === 'SystemModstamp') {
                cdmProp = 'externalTimestamp';
                propType = 'datetime';
            } else if(!hasSystemModstamp && bindProp.name === 'CreatedDate') {
                cdmProp = 'externalTimestamp';
                propType = 'datetime';
            }
            //console.log("Adding Binding for " + sfdcName + "." + bindProp.name)

            propertyBindingList.push({
                cdmType: cdmName,
                cdmProperty: cdmProp,
                externalType: sfdcName,
                externalProperty: bindProp.name,
                externalSchema: externalSystem
            });
        }

        typeBinding.properties = propertyBindingList;

        model.push({
            op: 'Invoke',
            targetType: 'CdmTypeBinding',
            name : "create",
            params:  {
                typeBinding: typeBinding
            }
        });
    }

    function addEnumeration(typeName,externalTypeName,props,callback) {
        // Adds or replaces a CDM enumeration based on a table in Saleforce
        var sfProps = new Object();
        sfProps[props.label] = true;
        sfProps[props.value] = true;
        for(var name in props.extraData) {
            sfProps[props.extraData[name]] = true;
        }
        var query = 'SELECT ';
        for(var name in sfProps) {
            if(query!='SELECT ') query += ',';
            query += name;
        }
        query += ' FROM ' + externalTypeName;

        var encodedQuery = encodeURIComponent(query);
        session.restRequest("GET", 'query/?q=' + encodedQuery, null, function(err, result) {
            if (err) {
                return callback(err);
            }
            var records = result.jsonBody.records;
            var enumeration = [];
            records.forEach(function(row) {
                var item = {
                    label:row[props.label],
                    value:row[props.value]
                };
                item.extraData = {};
                for(var name in props.extraData) {
                    item.extraData[name] = row[props.extraData[name]];
                }
                enumeration.push(item);
            });

            replaceEnumeration(typeName,enumeration);
            callback(null);
        });
    }

    function addWarning(message, code) {
        warnings.push({
            message: message,
            code : code,
            targetType: "CdmWarning",
            info: {}
        })
    }

}
