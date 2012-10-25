var emproxy = require('emproxy');
var sforce = require('../lib/emsfproxy');

emproxy.init(function afterInitCallback(initialConfig) {
    sforce.setInitialConfig(initialConfig);
    emproxy.start(sforce.processDirective);
});
