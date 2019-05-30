'use strict';

const Joi = require('joi');

const userSubscriptionSchema = Joi.object().keys({
  operation: Joi.string().valid(['delete', 'create']).required(),
  userId: Joi.string(),
  deviceId: Joi.string(),
  phone: Joi.string(),
  thresholdTemperature: Joi.number(),
  thresholdHumidity: Joi.number()
});

module.exports = userSubscriptionSchema;