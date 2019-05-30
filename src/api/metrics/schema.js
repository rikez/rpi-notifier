'use strict';

const Joi = require('joi');

const metricsSchema = Joi.object().keys({
  deviceId: Joi.string().required(),
  temperature: Joi.number().required(),
  humidity: Joi.number().required()
});

module.exports = metricsSchema;