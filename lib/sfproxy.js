var sfsess = require('./sforce');
var util = require('util');

var verbose;
var targetTypes;

exports.setInitialConfig = function setInitialConfig(initialConfig) {
	targetTypes = initialConfig.targetTypes;
	sfsess.settings.verbose = initialConfig.verbose;
	verbose = initialConfig.verbose;
	sfsess.settings.endpoint = initialConfig.endpoint;
}

// Validates that the request contains credentials, 
// establishes a Salesforce session with the credential,
// passes control to processOperation
exports.processDirective = function processDirective(requestData,callback) {
	var options = requestData.options;
	var restResult;
	if(!(options && options.credentials)) {
		restResult = new Object();
		restResult.targetType = 'RestResult';
		restResult.status = 'ERROR';
		restResult.errors = [{targetType:'CdmError',errorCode:'integration.login.fail.nocredentials',errorMessage:"No credentials have been entered"}];
		callback(null,restResult); // An error on validate credentials returns a normal restResponse
	} else {
		if(requestData.op === 'INVOKE' && requestData.targetType === "CdmExternalCredentials") {
			var testSession = new sfsess.Session(options.credentials);
			testSession.login(function(err) {
				restResult = new Object();
				restResult.targetType = 'RestResult';
				if(err) {
					restResult.status = 'ERROR';
					var errorCode = 'integration.login.fail';
					if(err.errorCode.indexOf('integration.login.fail') == 0) {
						errorCode = err.errorCode;
					}
					restResult.errors = [{targetType:'CdmError',errorCode:errorCode,errorMessage:err.message}];
				} else {
					restResult.status = 'SUCCESS';					
   				}
   				callback(null,restResult); // An error on validate credentials returns a normal restResponse
			});
		} else {	
			sfsess.session(options.credentials,function(err,session) {
				if(err) {
					callback(err);
				} else {			
					processOperation(session,requestData,function(err,responseData) {
						callback(err,responseData);
					});
				}
			});
		}
	}
}

function processOperation(session,directive,callback) {
	// callback should return (err,responseData)

	var errList = [];
	var method;
	var expectedStatus;
	var resource;

	if(typeof directive.targetType === 'undefined') {
		callback(new Error('Request must include targetType'));
		return;
	} 

	var typeInfo = targetTypes[directive.targetType];
	if(typeof typeInfo === 'undefined') {
		callback(new Error('No type info found for targetType='+directive.targetType));
		return;			
	}

	if(directive.op === 'INVOKE' && directive.targetType === "Document") {
        console.log("Directive for INVOKE on Document: " + util.inspect(directive,true,null));
		if(!directive.params || !directive.params.name) {
			var msg = "Document operation must include a name param"
			console.log(msg);
			callback(new Error(msg));	
			return;
		}
		if(directive.name == "fetch") {
			return fetchDocument(session,directive,callback);
		} else if(directive.name == "refresh") {
			return upsertDocument(session,directive,callback);
		} else {
			var msg = "Unsuported INVOKE operation: " + directive.name;
			console.log(msg);
			callback(new Error(msg));
			return;	
		}
	}

	var fetchDeletesSince = 0;

	convertInputValues(errList,directive.values,typeInfo);

	if(directive.op === 'SELECT') {
		method = 'GET';
		expectedStatus = 200;

		if(directive.options.cursorId) {
			resource = directive.options.cursorId;
			console.log('Fetching additional rows for: ' + directive.targetType + ' with ' + resource);
		} else {
			var query = "SELECT "+directive.properties.join(',')+" from "+directive.targetType;
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
				var soqlString = queryExprToSOQL(directive.where,typeInfo);
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

	session.restRequest(method,resource,directive.values,function(err,status,result) {
		if(status===400) {
			console.log('400, malformed request, resource=' + resource + ' values:');
			console.dir(directive.values);
		}
		if(err) {
			callback(err);
		} else if(status === expectedStatus) {
			restResult = new Object();
			restResult.targetType = 'RestResult';
			restResult.status = 'SUCCESS';
			restResult.externalId = result.id;
			if(method==='GET') {
				restResult.cursor = result.nextRecordsUrl;
				restResult.count = result.totalSize;
				restResult.results = convertToCdmResult(errList,result,typeInfo);
				if(restResult.cursor) {
					console.log('Query returned ' 
					+ result.records.length + ' ' 
					+ directive.targetType 
					+ ' and a cursor: ' 
					+ restResult.cursor);
				}
			}
			if(verbose) {
				console.log('Directive:');
				console.log(util.inspect(directive,true,null));
				console.log('Result:');
				console.log(util.inspect(restResult,true,null));
			}

			var now;

			if(fetchDeletesSince > 0) {
				// SFDC has an undocumented rule that startDate must be more than one minute less than endDate
				var now = new Date();
				if(now.getTime() - fetchDeletesSince < 60001) {
					fetchDeletesSince = now.getTime() - 60001;
				}
				session.getDeleted(directive.targetType
				,ISODateString(new Date(fetchDeletesSince))
				,ISODateString(now)
				,function(err,body) {
					if(err) {
						callback(err);
					} else {
						if(body.deletedRecords) {
							console.dir(body);
							addDeletesToCdmResult(errList,body.deletedRecords,restResult.results);
						}
						callback(null,restResult);
					}				
				});
			} else {
				callback(null,restResult);
			}
		} else  {
			// console.log("status=" + status + " " + resource);
			callback(new Error('Unsuccessful status code from ' + method + ': ' + status))	
		}			
	});
}

function getFolderId(session,fullPath,callback) {
    if(fullPath.charAt(0) == '/') {
        fullPath = fullPath.substr(1);
    }

    var path = fullPath.split('/');
	console.log("PATH = ");
	console.dir(path);
	if(path.length != 2) {
		callback(new Error('Folder was not included in resource name: ' + fullPath));
		return;
	}

	var folderName = path[0];
	var documentName = path[1];

	var query = "SELECT id,Name FROM Folder WHERE Name='" + folderName + "'";

	resource = 'query/?q=' + encodeURIComponent(query);
	
	session.restRequest('GET',resource,null,function(err,status,result) {
		if(err) {
			callback(err);
		} else if(status === 200) {
			restResult = new Object();
			restResult.targetType = 'RestResult';
			restResult.status = 'SUCCESS';

			if(result.totalSize > 0) { 
				var folderId = result.records[0].Id;
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
		
		session.restRequest('GET',resource,directive.values,function(err,status,result) {
			if(err) {
				callback(err);
			} else if(status === 200) {
				console.dir(result);

				restResult = new Object();
				restResult.targetType = 'RestResult';
				restResult.status = 'SUCCESS';

				if(result.totalSize > 0) { 
					//restResult.externalId = result.id;
					restResult.contentType = result.records[0].ContentType;
					restResult.url = result.records[0].Body;

					session.urlRequest('GET',restResult.url,null,function(err,status,docData) {
				        if(err) {
				            console.log('Saleforce integration error: ' );
							callback(err);			            
				        } else {
				            console.log('Saleforce returned status: ' + status);
				            console.log('Saleforce docData length = ' + docData.length);

				            restResult.content = docData.toString('base64');
							callback(null,restResult);
				        }       
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

		if(err) {
			callback(err);
			return;
		}

		directive.params.name = documentName;
		directive.params.folderId = folderId;

		session.upsertDocument(directive.params,function(err,status,result) {
			if(err) {
				callback(err);
			} else if(status === 200 || status === 201) {
				restResult = new Object();
				restResult.targetType = 'RestResult';
				restResult.status = 'SUCCESS';
				restResult.externalId = result.id;
				restResult.name = result.id; // was: fullNam
				callback(null,restResult)
			} else {
				callback(new Error('Unsuccessful status when inserting new Document: ' + status))	
			}
		});
	});
}

function convertInputValues(errList,values,typeInfo) {
	if(values) {
		for(var key in values) {
			var mappedProp = typeInfo.properties[key];
			if(typeof mappedProp === 'undefined') {
				errList.push(new Error('No type info found for targetType='+mappedType.target+' property='+key));
				mappedProp = 'UNMAPPED'
			} 
		}
	}
}

function convertOutputValues(errList,values, typeInfo) {
	var sfData = new Object();
	for(var key in values) {
		if(key === 'attributes') continue; // skip over Saleforce attributes
		var propInfo = typeInfo.properties[key];
		if(propInfo) {				
			if(propInfo.externalType === 'datetime') {
                sfData[key] = new Date(values[key]).getTime();
			} else {
			    sfData[key] = values[key];
			}
		}
	}
	return sfData;
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

function addDeleteToCdmResult(record,cdmResults) {
	var cdmRecord = new Object();
	cdmRecord.externalId = record.id;
	cdmRecord.cdmDeleted = true;
	cdmResults.push(cdmRecord);
}

function queryExprToSOQL(query,typeInfo) {
	var sb = [];
	for(var key in query) {
		var val = query[key];
		var clause = termToSOQL(key,val,typeInfo);
		sb.push(clause);
	}
	return sb.join(' AND ');
}

function termToSOQL(key,val,typeInfo) {
	var sb = '';

	if(key.charAt(0) === '$') {
		if(key.toLowerCase() === '$or' || key.toLowerCase() === '$and') {
			sb += '(';
			for (var i=0; i < val.length; i++) {
				var expr = val[i];
					var clause = queryExprToSOQL(expr,typeInfo);
					sb += clause;
				if(i < val.length-1) {
					if (key.toLowerCase() === '$or') {
						sb += ' OR ';
					} else {
						sb += ' AND ';
					}
				}
			}
			sb += ')';
		} else {
			throw new Error('Unsupported operator: ' + key);
		}
	} else {
		var info = typeInfo.properties[key];
		if(typeof info === 'undefined') {
			throw new Error('Missing property info in query: ' + key);				
		}
		var valueExpr = valueExprToSOQL(val,info.externalType);

		sb += key;
		sb += valueExpr;
	}
	return sb;
}

function valueExprToSOQL(expr,mappedType) {
	var op;
	var key;
	var value;

	if(typeof expr != 'object') {
		// assume this is a scalar value and the operator defaults to '='
		return '=' + scalarValueExprToSOQL(expr,mappedType);
	}
	
	for(var key in expr) {
		var value = expr[key];

		switch(key) {
			case '$gt':
				op = '>'
				break;
			case '$gte':
				op = '>='
				break;
			case '$lt':
				op = '<'
				break;
			case '$lte':
				op = '>='
				break;
			case '$ne':
				op = '!='
				break;
			case '$in':
				op = ' IN '
				break;
			case '$nin':
				op = ' NOT IN '
				break;
			case '$all':
				throw new Error("Unsupported operator " + key);
				break;
			case '$or':
				throw new Error("Unsupported operator in this context: " + key);
				break;
			case '$exists':
				throw new Error("Unsupported operator " + key);
				break;
			default:
				throw new Error("Expected operator, found " + key);
				break;
		}

		if(typeof value === 'object') {
			var vals = [];
			for(var subkey in value) {
				var subexpr = value[subkey];
				var clause = scalarValueExprToSOQL(subexpr,mappedType);
				vals.push(clause);
			}
			return op + '(' + vals.join(',') + ') ';
		} else {		
			return op + scalarValueExprToSOQL(value,mappedType);
		}
	}
}

function scalarValueExprToSOQL(val,mappedType) {
	if(mappedType == 'datetime') {
		if(typeof val === 'number') {
			var dt = new Date(val);
			return ISODateString(dt);
		} else {
			return val; // handle case of unquoted ISO date per SOQL syntax
		}
	} else if(mappedType == 'date') {
		if(typeof val === 'number') {
			var dt = new Date(val);
			return SOQLDateString(dt);
		} else {
			return val; // handle case of unquoted yyyy-mm-dd per SOQL syntax
		}
	} else if(typeof val === 'number' || typeof val === 'boolean') {
		return val;
	} else {
		return "'" + val + "'";
	}
}

function pad(n){
    return n<10 ? '0'+n : n
}

function ISODateString(d) {
    return d.getUTCFullYear()+'-'
    + pad(d.getUTCMonth()+1)+'-'
    + pad(d.getUTCDate())+'T'
    + pad(d.getUTCHours())+':'
    + pad(d.getUTCMinutes())+':'
    + pad(d.getUTCSeconds())+'Z'
}

function SOQLDateString(d) {
    return d.getUTCFullYear()+'-'
    + pad(d.getUTCMonth()+1)+'-'
    + pad(d.getUTCDate())
}



