'use strict';

const VError = require('verror');

const config = require('../../config');
const logger = require('../../infra/logger');
const schema = require('./schema');
const User = require('./model');

/**
 * @class MetricsHandler
 */
class UserSubscriptionHandler {
  constructor(userSubscriptionConsumer) {
    this.userSubscriptionConsumer = userSubscriptionConsumer;
  }

  /**
   * Handler for the Kafka Consumer
   * @memberof MetricsHandler
   */
  async init() {
    const onWarn = (e) => {
      logger.warn(new VError(e, 'Unexpected warn on the userSubscriptionConsumer'));
    };

    const onError = (e) => {
      logger.error(new VError(e, 'Unexpected error on the userSubscriptionConsumer'));
      process.kill(process.pid, 'SIGINT');
    };

    const onInfo = (msg, args) => {
      logger.info({ args }, 'UserSubscription: ' + msg);
    };

    this.userSubscriptionConsumer.on('warn', onWarn);
    this.userSubscriptionConsumer.on('error', onError);
    this.userSubscriptionConsumer.on('info', onInfo);

    await this.userSubscriptionConsumer.init({
      serviceName: config.NAME,
      topic: config.USER_SUBSCRIPTION_TOPIC,
      retries: 1,
      concurrency: config.USER_SUBSCRIPTION_TOPIC_CONCURRENCY,
      handler: (msg) => this.handler(msg)
    });
  }


  /**
   * Kafka message handler
   * @param {{ temperature: number, humidity: number, deviceId: string }} msg
   * @memberof MetricsHandler
   */
  async handler(msg) {
    logger.info({ payload: msg }, `Received a message in the topic: ${config.USER_SUBSCRIPTION_TOPIC}`);

    const result = schema.validate(msg);
    if (result.error) {
      logger.error(new VError(result.error, 'Failed to validate the message'));
      return;
    }

    const { operation, deviceId, userId, phone, thresholdTemperature, thresholdHumidity } = msg;

    if (operation == 'create') {
      const user = new User({ deviceId, userId, thresholdTemperature, thresholdHumidity, phone });
      const created = await user.save();

      logger.info({ user, created }, 'Created a new user');
    } else if (operation == 'delete') {
      const removed = await User.remove({ userId });

      logger.info({ removed }, 'Removed a user');
    }
  }
}

module.exports = UserSubscriptionHandler;