exports.mongoQuerytoSOQL = mongoQuerytoSOQL;
exports.ISODateString = ISODateString;
exports.SOQLDateString = SOQLDateString;

function mongoQuerytoSOQL(query,typeInfo) {
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
                    var clause = mongoQuerytoSOQL(expr,typeInfo);
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
        var info = typeInfo.fields[key];
        if(typeof info === 'undefined') {
            throw new Error('Missing property info in query: ' + key);              
        }
        var valueExpr = valueExprToSOQL(val,info.type);

        sb += key;
        sb += valueExpr;
    }
    return sb;
}

function valueExprToSOQL(expr,fieldType) {
    var op;
    var key;
    var value;

    if(typeof expr != 'object') {
        // assume this is a scalar value and the operator defaults to '='
        return '=' + scalarValueExprToSOQL(expr,fieldType);
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
                var clause = scalarValueExprToSOQL(subexpr,fieldType);
                vals.push(clause);
            }
            return op + '(' + vals.join(',') + ') ';
        } else {        
            return op + scalarValueExprToSOQL(value,fieldType);
        }
    }
}

function scalarValueExprToSOQL(val,fieldType) {
    if(fieldType == 'datetime') {
        if(typeof val === 'number') {
            var dt = new Date(val);
            return ISODateString(dt);
        } else {
            return val; // handle case of unquoted ISO date per SOQL syntax
        }
    } else if(fieldType == 'date') {
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



