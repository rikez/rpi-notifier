'use strict';

const EventEmitter = require('events');
const VError = require('verror');
const Kafka = require('node-rdkafka');

const sleep = (ms) => new Promise((resolve, reject) => setTimeout(resolve, ms));

/**
 * The BaseConsumer is a simple wrapper around the node-rdkafka consumer, supporting event handling, 
 * graceful shutdown, and proper concurrency on the consumption
 * @class BaseConsumer
 * @extends {EventEmitter}
 */
class BaseConsumer extends EventEmitter {
  /**
   * Creates an instance of BaseConsumer.
   * Check the original doc https://github.com/edenhill/librdkafka/blob/0.11.1.x/CONFIGURATION.md for the variety of possibilities
   * @param {RDKafkaBroker} brokerOpts 
   * @param {RDKafkaTopic} topicOpts
   * @param {Producer} producer
   * @memberof BaseConsumer
   */
  constructor(brokerOpts, topicOpts, producer) {
    super();

    // Properties Validation
    if (!brokerOpts || typeof brokerOpts !== 'object') {
      throw new VError('The rdKafkaBrokerOpts parameter must be an object');
    }
    if (topicOpts && typeof topicOpts !== 'object') {
      throw new VError('The topicOpts parameter must be an object');
    }

    // The Kafka broker options for the consumer
    this._brokerOpts = {
      'group.id': 'teravoz-default-group',
      'socket.keepalive.enable': true,
      'api.version.request': true,
      'api.version.fallback.ms': 0,
      'enable.auto.commit': false,
      'log.connection.close': false,
      'offset_commit_cb': (error, topicPartitions) => {
        if (error) {
          this.emit('committed-error', new VError(error, 'Failed to commit the offset'));
        } else {
          this.emit('committed', topicPartitions);
        }
      },
      ...brokerOpts
    };

    // The Kafka topic options for the consumer
    this._topicOpts = {
      'auto.offset.reset': 'latest',
      ...topicOpts
    };

    this._topicVariants = null;
    this._producer = producer;

    // Consumer state control variables
    this._processing = false;
    this._disposing = false;
    this._initialized = false;
  }


  /**
   * The post constructor initialization that ensures emitted events not to be lost
   * @param {string} serviceName
   * @param {string} topic
   * @param {number} [retries=0] [default=0]
   * @memberof BaseConsumer
   */
  async init(serviceName, topic, retries = 0, initAttempts = 1) {
    try {
      if (!serviceName || typeof serviceName !== 'string') {
        throw new VError('The serviceName parameter must be a string');
      }
      if (!topic || typeof topic !== 'string') {
        throw new VError('The topic parameter is a required string');
      }
      if (typeof retries !== 'number') {
        throw new VError('The retries parameter must be a number');
      }

      this._topicVariants = {
        topic,
        retriesTopic: `${topic}-${serviceName}-retries`,
        deadLetterTopic: `${topic}-${serviceName}-dl`,
        retries
      };

      await this._connect(this._brokerOpts, this._topicOpts);
    } catch (error) {
      if (initAttempts > 10) {
        throw new VError(error, 'Failed to initialize the KafkaConsumer');
      }

      this.emit('info', 'Trying to connect the consumer to the Kafka Broker');
      await sleep(initAttempts * 200);
      return await this.init(serviceName, topic, retries, ++initAttempts);
    }
  }

  /**
   * Subscribe consumer to the topics
   * @memberof BaseConsumer
   */
  subscribe() {
    this.consumer.subscribe([this._topicVariants.topic, this._topicVariants.retriesTopic]);
  }

  /**
   * Registers all of the event callbacks, exposing it via EventEmitter, and connects to the broker.
   * @private
   * @returns {Promise<void>}
   * @memberof BaseConsumer
   */
  _connect(brokerOpts, topicOpts) {
    return new Promise((resolve, reject) => {
      this.consumer = null;

      const consumer = new Kafka.KafkaConsumer(brokerOpts, topicOpts);

      this._createEventListeners(consumer);
     
      this.consumer = consumer;

      this.consumer.connect({ timeout: 5000, topic: this._topicVariants.topic }, (error, data) => {
        if (error) {
          return reject(new VError(error, 'Failed to open the connection with the Kafka Broker'));
        }
        
        this.emit('info', 'KafkaConsumer is ready');
        this._initialized = true;
        resolve();
      });
    });
  }


  /**
   * Reconnects to the Kafka Broker
   * @param {number} [retries=1]
   * @returns 
   * @memberof BaseConsumer
   */
  async _reconnect(retries = 1) {
    try {
      this._reconnecting = true;
      this._initialized = false;
      
      await this._connect(this._brokerOpts, this._topicOpts);

      this.subscribe();

      this._reconnecting = false;
      this.emit('info', 'Reconnected to the Kafka Broker');
    } catch (error) {
      if (retries > 6) {
        this.emit('error', new VError(error, 'Failed to reconnect to the Kafka Broker'));
        return;
      }

      await sleep(10000);
      this.emit('warn', `Attempt ${retries} of 6, to reconnect to the Kafka Broker...`);
      return await this._reconnect(++retries);
    }
  }


  /**
   * Set the event listeners
   * @param {Kafka.KafkaConsumer} consumer
   * @memberof BaseConsumer
   */
  _createEventListeners(consumer) {
    consumer.on('event.log', (log) => {
      this.emit('debug', 'Received debug message on KafkaConsumer', log);
    });
    consumer.on('event.stats', (stats) => {
      this.emit('info', 'Received stats from the KafkaConsumer', stats);
    });
    consumer.on('event.throttle', (throttle) => {
      this.emit('info', 'Received throttling event from the KafkaConsumer', throttle);
    });

    consumer.on('disconnected', () => {
      this._initialized = false;
      this.emit('disconnected', 'Disconnected the KafkaConsumer');
    });
    consumer.on('event.error', (error) => {
      if (error.message == 'all broker connections are down') {
        this.emit('debug', new VError(error, 'Received the false positive ALL_BROKERS_ARE_DOWN'));
      } else if (error.code == Kafka.CODES.ERRORS.ERR__TRANSPORT || error.message == 'broker transport failure') {
        if (this._initialized && !this._reconnecting) {
          this.emit('warn', new VError(error, 'Trying to reconnect to the broker...'));
          this.consumer.unsubscribe();
          this._reconnect();
        }
      } else if (error.code == Kafka.CODES.ERRORS.ERR__PARTITION_EOF) {
        this.emit('info', 'Received ERR__PARTITION_EOF', error);
      } else {
        const verror = new VError(error, 'Unexpected error on KafkaConsumer');
        this.emit('error', verror);
      }
    });
  }

  /**
   * Consumes messages from a topic, passing the message to the handler callback
   * @param {Function} handler 
   * @memberof Consumer
   */
  async consume(handler) {
    if (this._reconnecting) {
      await sleep(500);
      return await this.consume(handler);
    }

    if (!this._initialized) {
      return;
    }

    if (!handler || typeof handler != 'function') {
      throw new VError('The handler parameter is a required function');
    }

    let msg = null;
    try {
      this._processing = true;

      if (this._disposing) {
        this._processing = false;
        return;
      }

      msg = await this._consumeAsync();
      if (msg) {
        await this._handler(handler, msg);
      }
    } catch (error) {
      this.emit('warn', error);
    }

    return await this.consume(handler);
  }

  _consumeAsync() {
    return new Promise((resolve, reject) => {
      this.consumer.consume(1, (error, messages) => {
        if (error) {
          return reject(new VError({
            cause: error,
            info: { topicVariants: this._topicVariants }
          }, 'Failed to consume the messages'));
        }

        if (messages && messages.length == 1) {
          return resolve(messages[0]);
        }

        return resolve(null);
      });
    });
  }

  /**
   * A Wrapper around the client handler function
   * @private
   * @param {Function} handler
   * @param {Object} msg
   * @returns {Promise<void>}
   * @memberof BaseConsumer
   */
  async _handler(handler, msg) {
    let value = null;

    try {
      value = JSON.parse(msg.value.toString());

      let fn = null;
      if (value.headers && value.body) {
        fn = handler(value.body);
      } else {
        fn = handler(value);
      }

      if (fn instanceof Promise) {
        await fn;
      }
    } catch (error) {
      this.emit('warn', new VError({
        cause: error,
        info: { msg, value, topicVariants: this._topicVariants }
      }, 'Failed to handle the message on the KafkaConsumer'));

      if (this._producer) {
        this.emit('warn', 'Requeueing the message', { msg, value, topicVariants: this._topicVariants });
        this._requeue(value);
      }
    } finally {
      this.consumer.commitMessage(msg);
    }
  }

  /**
   * Requeue the failed message on the retries or the dead-letter topic
   * @param {Object} msg
   * @memberof BaseConsumer
   */
  _requeue(msg) {
    try {
      if (msg.headers && msg.body) {
        if (msg.headers.retries > 0) {
          --msg.headers.retries;
          this._producer.publish(this._topicVariants.retriesTopic, msg);
        } else {
          this.emit('warn', new VError({
            info: { msg, topicVariants: this._topicVariants }
          }, 'The message reached the limit of attempts, so it will be sent to the topic dead-letter'));

          this._producer.publish(this._topicVariants.deadLetterTopic, msg);
        }
      } else {
        this._producer.publish(this._topicVariants.retriesTopic, {
          body: msg,
          headers: { retries: this._topicVariants.retries }
        });
      }
    } catch (error) {
      this.emit('error', new VError({
        cause: error,
        info: { msg, topicVariants: this._topicVariants }
      }, 'Failed to publish message on the topic'));
    }
  }

  /**
   * Disconnects from the broker
   * @returns {Promise<void>}
   * @memberof BaseConsumer
   */
  _disconnectAsync() {
    return new Promise((resolve, reject) => {
      this.consumer.disconnect((error, data) => {
        if (error) {
          return reject(new VError(error, 'Failed to disconnect from the KafkaConsumer'));
        }

        resolve(data);
      });
    });
  }

  /**
   * Disposes the connection with the KafkaConsumer. Depending on the consumer flow, it might take a while to dispose, due to the GC.
   * @returns
   * @memberof Consumer
   */
  async dispose() {
    if (!this._initialized) {
      return;
    }

    this._disposing = true;

    while (this._processing) {
      await sleep(100);
    }

    await this._disconnectAsync();
  }
}

module.exports = BaseConsumer;
