'use strict';

const VError = require('verror');

const config = require('../../config');
const logger = require('../../infra/logger');
const schema = require('./schema');
const User = require('../user/model');

/**
 * @class MetricsHandler
 */
class MetricsHandler {
  constructor(metricsConsumer, kafkaProducer) {
    this.metricsConsumer = metricsConsumer;
    this.kafkaProducer = kafkaProducer;
  }

  /**
   * Handler for the Kafka Consumer
   * @memberof MetricsHandler
   */
  async init() {
    const onWarn = (e) => {
      logger.warn(new VError(e, 'Unexpected warn on the metricsConsumer'));
    };

    const onError = (e) => {
      logger.error(new VError(e, 'Unexpected error on the metricsConsumer'));
      process.kill(process.pid, 'SIGINT');
    };

    const onInfo = (msg, args) => {
      logger.info({ args }, 'Metrics: ' + msg);
    };

    this.metricsConsumer.on('warn', onWarn);
    this.metricsConsumer.on('error', onError);
    this.metricsConsumer.on('info', onInfo);

    await this.metricsConsumer.init({
      serviceName: config.NAME,
      topic: config.NOTIFICATION_TOPIC,
      retries: 1,
      concurrency: config.NOTIFICATION_TOPIC_CONCURRENCY,
      handler: (msg) => this.handler(msg)
    });
  }

  /**
   * Kafka message handler
   * @param {{ temperature: number, humidity: number, deviceId: string }} msg
   * @memberof MetricsHandler
   */
  async handler(msg) {
    logger.info({ payload: msg }, `Received a message in the topic: ${config.NOTIFICATION_TOPIC}`);

    const result = schema.validate(msg);
    if (result.error) {
      logger.error(new VError(result.error, 'Failed to validate the message'));
      return;
    }

    const found = await User.findOne({ deviceId: msg.deviceId });
    if (!found) {
      logger.warn(msg, `An user with the deviceId ${msg.deviceId} was not found`);
      return;
    }

    if (msg.temperature > found.thresholdTemperature || msg.humidity/100 > found.thresholdHumidity) {
      // Use Dialer
    }

    logger.info({ user: found, msg }, `Found user with the deviceId ${msg.deviceId}`);

    const newMetrics = Object.assign({}, msg, { userId: found.userId });
    await this.kafkaProducer.publish(config.ELK_NOTIFICATION, newMetrics);
  }
}

module.exports = MetricsHandler;