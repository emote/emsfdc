"ust strict";

var httpRequest = require('emsoap').subsystems.httpRequest;
var util = require('util');
var xml2js = require('xml2js');
var emutils = require('emutils');

var tokenLifetime = 1000 * 60 * 45;  // SalesForce tokens are good for an hour.  Leave some cushion

function soapResource() {return '/services/Soap/u/' + settings.sfVersion;}
function resourceRoot() {return '/services/data/v' + settings.sfVersion + '/';}

var sessionCache = new Object();

var settings = new Object();
exports.settings = settings;

exports.getSession = getSession;

function getSession(credentials,callback /* (err,session) */) {

    if(!(credentials
        && credentials.accessToken
        && credentials.rawResponse))
    {
        callback(new Error('Request must contain Salesforce OAuth credentials.'));
    }

    if ((Date.now() - credentials.rawResponse.issued_at) > tokenLifetime) {
        callback(null, emutils.generateCredentialsError('The OAuth access token is expired', 'integration.login.fail.oauthTokenExpired'));
    }

    var sess = sessionCache[credentials.refreshToken];
    if (!sess) {
        sess = sessionCache[credentials.refreshToken] = new Session(credentials);
    }
    else {
        sess.update(credentials);
    }

    callback(null,sess);
}

function Session(credentials) {
    this.sfVersion = settings.sfVersion;
    this.metadata = {};
    this.update(credentials)
}

Session.prototype.update = function (credentials) {
    this.accessToken = credentials.accessToken;
    this.tokenIssuedAt = credentials.rawResponse.issued_at;
    this.sfHostUrl = credentials.rawResponse.instance_url;
}

Session.prototype.urlRequest = function(resource,callback /* (err,result) */) {
    urlRequest(this,resource,callback);
}

Session.prototype.soapRequest = function(resource,body,callback /* (err,result) */) {
    soapRequest(this,resource,body,callback);
}

Session.prototype.getDeleted = function(objectType,startDate,endDate,callback /* (err,result) */) {
    getDeleted(this,objectType,startDate,endDate,callback);
}

Session.prototype.restRequest = function(method,resource,query,callback /* (err,result) */) {
    restRequest(this,method,resource,query,callback);
}

Session.prototype.describeSObject = function(objectName,cb /* (err,metadata) */) {
    describeSObject(this,objectName,cb);
}

Session.prototype.getMetadataForTargetType = function(targetType,callback /* (err,typeInfo) */ ) {
    getMetadataForTargetType(this,targetType,callback);
}

Session.prototype.fetchDocument = function(directive,callback) {
    fetchDocument(this,directive,callback);
}

Session.prototype.upsertDocument = function(directive,callback) {
    upsertDocument(this,directive,callback);
}

function resourceRequest(session,method,headers,resource,body,callback /* (err,result) */) {
    var url = session.sfHostUrl + resource;

    var options = httpRequest.parseUrl(url);
    if(!options) {
        var err = new Error("error parsing url: " + url);
        console.log(err.message);
        process.nextTick(function() {
            callback(err);
        });
    }

    options.method = method;
    options.headers = addAuthHeader(headers, session.accessToken);

    httpRequest.httpRequest(options,body,function(err,result) {
        if(!result && err) {
            var err;
            if(err.httpStatusCode === 404) {
                err = new Error("DNS lookpup failed for URL: " + url);
            } else {
                err = {message: 'Unexpected error on request to Salesforce', cause: err};
            }
            return callback(err);
        }
        callback(err,result);
    });
}

function urlRequest(session,resource,callback /* (err,result) */) {
    var headers = {
        'Connection': 'keep-alive'
    };
    resourceRequest(session,'GET',headers,resource,null,callback);
}

var xmlParserOptions = {
    normalize: false,
    trimming: false,
    explicitRoot: true,
    xmlns: false,
    attrkey: "@",
    charkey: "#",
    explicitArray: false
};

function soapRequest(session,resource,body,callback /* (err,result) */) {

    var msg = [];

    msg.push('<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:urn="urn:partner.soap.sforce.com">');

    if(session) {
        msg.push('<soapenv:Header>');
        msg.push('<urn:SessionHeader>');
        msg.push('<urn:sessionId>');
        msg.push(session.accessToken);
        msg.push('</urn:sessionId>');
        msg.push('</urn:SessionHeader>');
        msg.push('<urn:DebuggingHeader><urn:debugLevel>DebugOnly</urn:debugLevel></urn:DebuggingHeader>');
        msg.push('<urn:DebuggingInfo><urn:debugLog>Debug</urn:debugLog></urn:DebuggingInfo>');
        msg.push('</soapenv:Header>');
    }

    msg.push('<soapenv:Body>');
    msg.push(body);
    msg.push('</soapenv:Body>');
    msg.push('</soapenv:Envelope>');

    var envelope = msg.join('');

    var headers = {
        'Content-Type': 'text/xml;charset=UTF-8',
        'Content-Length': Buffer.byteLength(envelope,'utf8'),
        'SOAPAction': '""'
    };

    resourceRequest(session,'POST',headers,resource,envelope,function(err,result) {
        if(err) {
            console.log(err);
            return callback(err);
        }
        var parser = new xml2js.Parser(xmlParserOptions);
        parser.parseString(result.body, function (err, root) {
            if(err) {
                return callback({
                        code:'integration.login.fail.parse',
                        message:'Cannot parse SOAP response from Salesforce',
                        cause: err
                });
            }

            var envelope = root['soapenv:Envelope'];
            if(!envelope) {
                return callback({
                    code:'integration.login.fail.soap',
                    message:'No SOAP Envelope found in response from Salesforce',
                    dom: root
                });
            }

            var body = envelope['soapenv:Body'];
            if(!body) {
                return callback({
                    code:'integration.login.fail.body',
                    message:'No body found in SOAP response from Salesforce',
                    dom: root
                });
            }

            var fault = body['soapenv:Fault'];
            if(fault) {
                var err = {
                    code:'integration.soapFault',
                    message: (fault.faultstring  || "Soap fault has no faultstring!")
                };
                // Map Salesforce faultcode to an error code that is understood by the MMC
                if(fault.faultcode && (fault.faultcode.indexOf('INVALID_LOGIN')) >=0) {
                    err.code = 'integration.login.fail';
                }
                return callback(err,root);
            }

            result.xmlRoot = root;
            result.xmlBody = body;

            callback(null,result);
        });
    });
}

function getDeleted(session,objectType,startDate,endDate,callback /* (err,result) */) {

    var soapBody =
        '<urn:getDeleted xmlns:urn="urn:partner.soap.sforce.com">' +
            '<urn:sObjectType>' + objectType + '</urn:sObjectType>' +
            '<urn:startDate>' + startDate + '</urn:startDate>' +
            '<urn:endDate>' + endDate + '</urn:endDate>' +
        '</urn:getDeleted>';

    soapRequest(session,soapResource(),soapBody,function(err,result) {
        if(result.xmlBody.getDeletedResponse) {
            var deletedList = result.xmlBody.getDeletedResponse.result;
            callback(null,deletedList);
        } else {
            err = new Error('Did not receive expected response from Salesforce for getDeleted.');
            callback(err);
        }
    });
}

function soapUpsertDocument(session,params,callback /* (err,result) */) {

    var soapBody =
        '<urn:upsert xmlns:urn="urn:partner.soap.sforce.com">' +
            '<urn:externalIDFieldName>Name</urn:externalIDFieldName>' +
            '<urn:sObjects>' +
                '<urn1:type xmlns:urn1="urn:sobject.partner.soap.sforce.com">Document</urn1:type>' +
                '<Body>' + params.content + '</Body>' +
                '<ContentType>' + params.contentType + '</ContentType>' +
                '<Name>' + params.name + '</Name>' +
                '<DeveloperName>' + params.name + '</DeveloperName>' +
                '<FolderId>' + params.folderId + '</FolderId>' +
            '</urn:sObjects>' +
        '</urn:upsert>';

    soapRequest(session,soapResource(),soapBody,function(err,result) {
        if(result.xmlBody.upsertResponse) {
            var sfResult = result.xmlBody.upsertResponse.result;
            if(sfResult && sfResult.errors) {
                callback(new Error("Error message returned from Salesforce.com: " + util.inspect(sfResult.errors,true,null)));
            } else {
                callback(null,sfResult);
            }
        } else {
            err = new Error('Did not receive expected response from Salesforce for upsertDocument.');
            console.log(err);
            console.dir(result);
            callback(err);
        }
    });
}

function restRequest(session,method,resource,query,callback /* (err,result) */) {

    var requestBody = null;
    if(query) {
        requestBody = JSON.stringify(query);
    }

    var headers = addAuthHeader({
        'Connection': 'keep-alive',
        'Content-Type': 'application/json'
    }, session.accessToken);

    if(method != 'GET' && requestBody) {
        headers['Content-Length'] = Buffer.byteLength(requestBody,'utf8');
    }

    var path = resource;
    if(path.charAt(0) != '/') {
        path = resourceRoot() + path;
    }

    resourceRequest(session,method,headers,path,requestBody,function(err,result) {
        if(err) return callback(err);

        if(result.body.length > 0) {
            try {
                // Non-error response should be JSON
                result.jsonBody = JSON.parse(result.body);
             } catch(err) {
                // any response other than JSON is likely an internal server error, report it
                var msg = 'Response to Salesforce REST call was not JSON: ' + result.body
                console.log(msg);
                var err = new Error(msg);
                return callback(err,result); // body is the error response
            }
        }

        callback(err,result);
    });
}

function describeSObject(session,objectName,cb /* (err,metadata) */) {

    restRequest(session,'GET','sobjects/'+objectName+'/describe/',null,function(err,result) {
        if(err) return cb(err);
        if(result.jsonBody.name != objectName) {
            console.log(status + ' Unexpected reply from Salesforce sobjects/ ');
            console.dir(result);
            return cb(new Error('Unexpected reply from Salesforce sobjects'));
        }

        var metadata = {name:result.jsonBody.name, fields:{}};

        result.jsonBody.fields.forEach(function(field) {
            metadata.fields[field.name] = field;
        });

        cb(null,metadata);
    });
}

function getMetadataForTargetType(session,targetType,callback /* (err,typeInfo) */ ) {
    if(session.metadata[targetType]) return callback(null,session.metadata[targetType]);

    describeSObject(session,targetType,function(err,metadata) {
        if(err) return callback(err);

        session.metadata[targetType] = metadata;
        callback(null,metadata);
    });
}

function getFolderId(session,fullPath,callback /* (err,folderId,documentName) */) {
    if(fullPath.charAt(0) == '/') {
        fullPath = fullPath.substr(1);
    }

    var path = fullPath.split('/');
    if(path.length != 2) {
        callback(new Error('Folder was not included in resource name: ' + fullPath));
        return;
    }

    var folderName = path[0];
    var documentName = path[1];

    var query = "SELECT id,Name FROM Folder WHERE Name='" + folderName + "'";

    resource = 'query/?q=' + encodeURIComponent(query);

    restRequest(session,'GET',resource,null,function(err,result) {
        if(err) return callback(err);

        if(result.status === 200) {
            if(result.jsonBody.totalSize > 0) {
                var folderId = result.jsonBody.records[0].Id;
                callback(null,folderId,documentName);
            } else {
                callback(new Error('Named folder not found for resource: ' + fullPath));
            }
        } else  {
            callback(new Error('Named folder not found for resource: ' + fullPath + ' status= ' + status));
        }
    });
}

function fetchDocument(session,directive,callback) {

    getFolderId(session,directive.params.name,function(err,folderId,documentName) {

        if(err) {
            callback(err);
            return;
        }

        var query = "SELECT id,Name,Body,ContentType FROM Document WHERE Name='"
            + documentName + "' AND FolderId = '" + folderId + "'";

        resource = 'query/?q=' + encodeURIComponent(query);

        restRequest(session,'GET',resource,null,function(err,result) {
            if(err) return callback(err);

            if(result.status === 200) {

                restResult = {
                    targetType: 'RestResponse',
                    status: 'SUCCESS'
                };

                if(result.jsonBody.totalSize > 0) {
                    restResult.contentType = result.jsonBody.records[0].ContentType;
                    restResult.url = result.jsonBody.records[0].Body;

                    urlRequest(session,restResult.url,function(err,result) {
                        if(err) return callback(err);

                        var docData = result.body; // this is a Buffer object
                        restResult.content = docData.toString('base64');
                        callback(null,restResult);
                    });

                } else {
                    // FAILED TO FIND OBJECT, not an error, per se...
                    callback(null,restResult);
                }
            } else  {
                callback(new Error('Unsuccessful status code from ' + method + ': ' + status));
            }
        });
    });
}

function upsertDocument(session,directive,callback) {
    if(!directive.params || !directive.params.contentType || !directive.params.content) {
        var msg = "Document refresh  operation must include a contentType and content params"
        console.log(msg);
        callback(new Error(msg));
        return;
    }

    var fullName = directive.params.name;

    getFolderId(session,fullName,function(err,folderId,documentName) {
        if(err) return callback(err);

        directive.params.name = documentName;
        directive.params.folderId = folderId;

        soapUpsertDocument(session,directive.params,function(err,result) {
            if(err) return callback(err);
            if(result.success) {
                var restResult = {
                    targetType: 'RestResponse',
                    status: 'SUCCESS',
                    externalId: result.id,
                    name: result.id
                };
                callback(null,restResult);
            } else {
                callback(new Error("Unsuccessful status when inserting new Document: " + util.inspect(result,true,null)),null);
            }
        });
    });
}

function addAuthHeader(headers, token) {
    headers = headers ? emutils.clone(headers) : {};
    headers.Authorization =  "Bearer " + token;
    return headers;
}

