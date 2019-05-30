'use strict';

const Joi = require('joi');

const userSubscriptionSchema = Joi.object().keys({
  operation: Joi.string().valid(['delete', 'create']).required(),
  userId: Joi.number(),
  deviceId: Joi.number(),
  phone: Joi.string(),
  thresholdTemperature: Joi.number(),
  thresholdHumidity: Joi.number()
});

module.exports = userSubscriptionSchema;