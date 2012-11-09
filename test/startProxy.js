var emproxy = require('emproxy');
var sforce = require('../lib/emsfproxy');

emproxy.init(function afterInitCallback(initialConfig) {
    emproxy.start(sforce.processDirective);
});
