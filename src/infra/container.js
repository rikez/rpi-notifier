'use strict';

// Libraries
const { asClass, asValue, createContainer } = require('awilix');

const MongoClient = require('../../lib/mongo/client');
const Producer = require('../../lib/kafka/producer');
const Consumer = require('../../lib/kafka/consumer');

// Microservice configuration
const config = require('../config');

// Handlers
const MetricsHandler = require('../api/metrics/handler');
const UserSubscriptionHandler = require('../api/user/handler');

/**
 * Registers the service container for the application
 * @returns
 */
function registerContainer() {
  const container = createContainer();
  container.register({
    mongoURI: asValue(config.MONGO_URI),
    mongoOpts: asValue({ useNewUrlParser: true, useCreateIndex: true }),
    mongoClient: asClass(MongoClient).classic().singleton().disposer((m) => m.dispose()),
    kafkaProducer: asClass(Producer).classic().singleton().disposer((p) => p.dispose()),
    kafkaProducerOpts: asValue({
      rdKafkaBrokerOpts: {
        'bootstrap.servers': config.KAFKA_SERVERS
      }
    }),
    metricsConsumer: asClass(Consumer).classic().singleton().disposer((c) => c.dispose()),
    userSubscriptionConsumer: asClass(Consumer).classic().singleton().disposer((c) => c.dispose()),
    kafkaConsumerOpts: asValue({
      rdKafkaBrokerOpts: {
        'bootstrap.servers': config.KAFKA_SERVERS,
        'group.id': config.NAME
      },
      rdKafkaTopicOpts: {
        'auto.offset.reset': process.env.NODE_ENV == 'test' ? 'earliest' : 'latest'
      }
    }),
    metricsHandler: asClass(MetricsHandler).classic().singleton(),
    userSubscriptionHandler: asClass(UserSubscriptionHandler).classic().singleton()
  });

  return container;
}

module.exports = registerContainer;
