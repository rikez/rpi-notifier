'use strict';

/* istanbul ignore file */

require('dotenv').config(process.env.NODE_ENV == 'test' ? { path: './.env.test' } : null);

const Joi = require('joi');

const configSchema = Joi.object({
  NAME: Joi.string().default('rpi-router'),
  NOTIFICATION_TOPIC: Joi.string().required(),
  NOTIFICATION_TOPIC_CONCURRENCY: Joi.number().required(),
  USER_SUBSCRIPTION_TOPIC: Joi.string().required(),
  USER_SUBSCRIPTION_TOPIC_CONCURRENCY: Joi.number().required(),
  ELK_NOTIFICATION: Joi.string().required(),
  MONGO_URI: Joi.string().required(),
  KAFKA_SERVERS: Joi.string().required()
});

const config = {
  NAME: process.env.NAME,
  KAFKA_SERVERS: process.env.KAFKA_SERVERS,
  NOTIFICATION_TOPIC: process.env.NOTIFICATION_TOPIC,
  NOTIFICATION_TOPIC_CONCURRENCY: process.env.NOTIFICATION_TOPIC_CONCURRENCY,
  USER_SUBSCRIPTION_TOPIC: process.env.USER_SUBSCRIPTION_TOPIC,
  USER_SUBSCRIPTION_TOPIC_CONCURRENCY: process.env.USER_SUBSCRIPTION_TOPIC_CONCURRENCY,
  ELK_NOTIFICATION: process.env.ELK_NOTIFICATION,
  MONGO_URI: process.env.MONGO_URI
};

const result = configSchema.validate(config);
if (result.error) {
  console.error(result.error);
  process.exit(1);
}

module.exports = result.value;
