'use strict';

const { startRestServer } = require('./rest-server');

startRestServer(Number(process.env.PORT || 3001));
