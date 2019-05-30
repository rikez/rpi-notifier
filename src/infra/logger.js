'use strict';

const Pino = require('pino');

const logger = Pino({
  name: process.env.NAME,
  level: 'info',
  prettyPrint: process.env.LOGGER_DEV == 'true',
  enabled: !(process.env.NODE_ENV == 'test'),
  useLevelLabels: true
});

module.exports = logger;