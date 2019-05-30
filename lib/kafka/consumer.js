'use strict';

const EventEmitter = require('events');
const VError = require('verror');

const BaseConsumer = require('./base-consumer');

/**
 * Wrapper handler around the BaseConsumer
 * @class Consumer
 * @extends {EventEmitter}
 */
class Consumer extends EventEmitter {
  /**
   * Creates an instance of Consumer.
   * @param {{ rdKafkaConsumerOpts: Object, rdKafkaConsumerTopicOpts: Object }} kafkaConsumerOpts  Check out the available 
   * parameters for the RDKafka Consumer broker and topic options https://github.com/edenhill/librdkafka/blob/0.11.1.x/CONFIGURATION.md 
   * @param {Producer} producer
   * @memberof Consumer
   */
  constructor(kafkaConsumerOpts, kafkaProducer) {
    super();

    // Properties Validation
    if (!kafkaConsumerOpts || typeof kafkaConsumerOpts !== 'object') {
      throw new VError('The kafkaConsumerOpts parameter must be an object');
    }
    if (!kafkaConsumerOpts.rdKafkaBrokerOpts || typeof kafkaConsumerOpts.rdKafkaBrokerOpts !== 'object') {
      throw new VError('The kafkaConsumerOpts.rdKafkaBrokerOpts parameter must be an object');
    }
    if (kafkaConsumerOpts.rdKafkaTopicOpts && typeof kafkaConsumerOpts.rdKafkaTopicOpts !== 'object') {
      throw new VError('The kafkaConsumerOpts.rdKafkaTopicOpts parameter must be an object');
    }

    this._brokerOpts = kafkaConsumerOpts.rdKafkaBrokerOpts;
    this._topicOpts = kafkaConsumerOpts.rdKafkaTopicOpts || {};
    this._producer = kafkaProducer;
    this._consumers = [];
  }

  /**
   * The post constructor initialization that ensures emitted events not to be lost
   * @param {{ serviceName: string, topic: string, handler: Function, concurrency: number, retries?: number }} config 
   * @memberof Consumer
   */
  async init(config) {
    try {
      if (!config || typeof config !== 'object') {
        throw new VError('The config parameter must be an object');
      }
      const { serviceName, topic, handler, retries, concurrency } = config;

      if (!topic || typeof topic !== 'string') {
        throw new VError('The config.topic parameter must be a string');
      }
      if (retries != null && retries != undefined && typeof retries !== 'number') {
        throw new VError('The config.retries parameter must be a number');
      }
      if (!serviceName || typeof serviceName !== 'string') {
        throw new VError('The config.serviceName parameter must be a string');
      }
      if (!concurrency || typeof concurrency !== 'number') {
        throw new VError('The config.concurrency parameter must be a number');
      }
      if (!handler || typeof handler !== 'function') {
        throw new VError('The config.handler parameter must be a function');
      }
      
      for (let i = 0; i < concurrency; i++) {
        const prefix = `Internal Consumer(${i})`;
        const baseConsumer = new BaseConsumer(this._brokerOpts, this._topicOpts, this._producer);

        baseConsumer.on('debug', (msg, args) => this.emit('debug', msg, args));
        baseConsumer.on('info', (msg, args) => {
          this.emit('info', `${prefix}: ${msg}`, args);
        }); 
        baseConsumer.on('committed', (topicPartition) => {
          this.emit('info', `${prefix}: The offset was committed`, topicPartition);
        });
        baseConsumer.on('committed-error', (error) => {
          this.emit('error', error);
        });
        baseConsumer.on('disconnected', () => {
          this.emit('info', `${prefix} was disconnected`);
        });
        baseConsumer.on('warn', (error) => {
          this.emit('warn', new VError(error, 'A warning event was emitted by an internal consumer'));
        });
        baseConsumer.on('error', (error) => this.emit('error', error));

        await baseConsumer.init(serviceName, topic, retries);

        this._consumers.push(baseConsumer);
      }

      for (const consumer of this._consumers) {
        consumer.subscribe();
        consumer.consume(handler);
      }
    } catch (error) {
      throw new VError(error, 'Failed to initialize the KafkaConsumer');
    }
  }

  /**
   * Disposes the connection with the KafkaConsumer. Depending on the consumer flow, it might take a while to dispose, due to the GC.
   * @returns
   * @memberof Consumer
   */
  async dispose() {
    await Promise.all(this._consumers.map((consumer) => consumer.dispose()));
    this._consumers = [];
  }
}

module.exports = Consumer;
