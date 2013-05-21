"use strict";
var emutils = require('emutils');

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
            case '$regex':
                var likeExpr = convertRegex(value);
                if (likeExpr == null) {
                    throw new Error("Unsupported regular expression '" + value + "'");
                }
                else {
                    value = likeExpr;
                    op = " LIKE ";
                }
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

// If possible, convert the regex to a LIKE expression
// The transformations are:
//  1. A regex that's not anchored gets a '%' at its front and/or back
//  2. . becomes _
//  3. .* becomes %
//  4. .+ becomes _% (at least one character)
//  5. Any other regex syntax is unsupported
function convertRegex(regex) {
    if (emutils.type(regex) != 'string') {
        return null;
    }
    else if (regex.length == 0) {
        return '%';
    }

    var len = regex.length;

    var front = '';
    var back = '';

    if (regex.substring(0, 1) == '^') {
        regex = regex.substr(1);
        len = regex.length;
    }
    else  {
        front = '%';
    }
    if (regex.substring(len-1, len) == '$') {
        regex = regex.substr(0, len-1);
        len = regex.length;
    }
    else {
        back = '%';
    }

    var likeExpr = "";
    for (var i = 0; i < regex.length; i++) {
        var c = regex.charAt(i);
        switch(c) {
            case '\\':
                likeExpr = addToLikeExpr(likeExpr, regex.char(++i));
                break;

            default:
                likeExpr = addToLikeExpr(likeExpr, c);
                break;

            case '.':
                if (i < (regex.length  - 1)) {
                    if (regex.charAt(i+1) == '*') {
                        likeExpr += '%';
                        i++;
                        break;
                    }
                    else if (regex.charAt(i+1) == '+') {
                        likeExpr += '_%';
                        i++;
                        break;
                    }
                }
                likeExpr += '_';
                break;

            case '^':
            case '$':
            case '*':
            case '+':
            case '?':
            case '=':
            case '!':
            case ':':
            case '|':
            case '/':
            case '(':
            case ')':
            case '[':
            case ']':
            case '{':
            case '}':
                return null;
        }
    }

    return front + likeExpr + back;
}

function addToLikeExpr(expr, c) {
    switch(c) {
        case "'":
        case '"':
        case '\\':
        case '%':
        case '_':
            expr += ('\\' + c);
            break;

        case '\n':
            expr += '\\n';
            break;

        case '\r':
            expr += '\\r';
            break;

        case '\f':
            expr += '\\f';
            break;

        case '\t':
            expr += '\\t';
            break;

        default:
            expr += c;
            break;
    }

    return expr;
}



