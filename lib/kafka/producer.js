'use strict';

const EventEmitter = require('events');
const Kafka = require('node-rdkafka');
const VError = require('verror');

const sleep = (ms) => new Promise((resolve, reject) => setTimeout(resolve, ms));

/**
 * Wrapper over the RdKafka Producer
 * @class Producer
 * @extends {EventEmitter}
 */
class Producer extends EventEmitter {
  /**
   * Creates an instance of Producer.
   * @param {{rdKafkaProducerOpts: Object, rdKafkaProducerTopicOpts: Object }} kafkaProducerOpts
   * Check out the available parameters for the RdKafka Producer https://github.com/edenhill/librdkafka/blob/0.11.1.x/CONFIGURATION.md 
   * @memberof Producer
   */
  constructor(kafkaProducerOpts) {
    super();

    if (!kafkaProducerOpts || typeof kafkaProducerOpts !== 'object') {
      throw new VError('The kafkaProducerOpts parameter must be an object');
    }
    if (!kafkaProducerOpts.rdKafkaBrokerOpts || typeof kafkaProducerOpts.rdKafkaBrokerOpts !== 'object') {
      throw new VError('The kafkaProducerOpts.rdKafkaBrokerOpts parameter must be an object');
    }
    if (kafkaProducerOpts.rdKafkaTopicOpts && typeof kafkaProducerOpts.rdKafkaTopicOpts !== 'object') {
      throw new VError('The kafkaProducerOpts.rdKafkaTopicOpts parameter must be an object');
    }

    this._kafkaProducerOpts = kafkaProducerOpts.rdKafkaBrokerOpts;
    this._kafkaProducerTopicOpts = kafkaProducerOpts.rdKafkaTopicOpts;
    this._initialized = false;
    this._disposing = false;
    this._processing = false;

    this._publishAttempts = 5;
  }


  /**
   * Post-initialization
   * @memberof Producer
   */
  async init(initAttempts = 1) {
    try {
      await this._connect(this._kafkaProducerOpts, this._kafkaProducerTopicOpts);
    } catch (err) {
      if (initAttempts > 10) {
        throw new VError(err, 'Failed to setup Kafka producer client');
      }

      this.emit('info', 'Trying to connect the producer to the Kafka Broker');
      await sleep(initAttempts * 200);
      return await this.init(++initAttempts);
    }
  }


  /**
   * Setup the connection with the RdKafka Producer
   * @private
   * @returns
   * @memberof Producer
   */
  _connect(kafkaProducerOpts, kafkaProducerTopicOpts) {
    return new Promise((resolve, reject) => {
      this.producer = null;

      const producer = new Kafka.Producer({
        'client.id': 'kafka-teravoz',
        'dr_cb': true,
        'dr_msg_cb': true,
        'api.version.request': true,
        'api.version.fallback.ms': 0,
        'socket.keepalive.enable': true,
        'log.connection.close': false,
        ...kafkaProducerOpts
      }, {
        'produce.offset.report': true,
        ...kafkaProducerTopicOpts
      });


      this._createEventListeners(producer);
      producer.setPollInterval(100);

      this.producer = producer;
      this.producer.connect({ timeout: 5000 }, (error, data) => {
        if (error) {
          return reject(new VError(error, 'Failed to open the connection with the Kafka Broker'));
        }

        this.emit('info', 'KafkaProducer is ready');
        this._initialized = true;
        resolve();
      });
    });
  }

  

  /**
   * Reconnects to the Kafka Broker
   * @param {number} [retries=1]
   * @memberof Producer
   */
  async _reconnect(retries = 1) {
    try {
      this._reconnecting = true;
      this._initialized = false;

      await this._connect(this._kafkaProducerOpts, this._kafkaProducerTopicOpts);

      this._reconnecting = false;
      this.emit('info', 'The Producer reconnected to the Kafka Broker');
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
   * @param {Kafka.KafkaProducer} producer
   * @memberof Producer
   */
  _createEventListeners(producer) {
    producer.on('event.log', (log) => {
      this.emit('info', 'Received debug message on KafkaProducer', log);
    });
    producer.on('event.error', (error) => {
      if (error.message == 'all broker connections are down') {
        this.emit('debug', new VError(error, 'Received the false positive ALL_BROKERS_ARE_DOWN'));
      } else if (error.code == Kafka.CODES.ERRORS.ERR__TRANSPORT || error.message == 'broker transport failure') {
        if (this._initialized && !this._reconnecting) {
          this.emit('warn', new VError(error, 'Trying to reconnect to the broker...'));
          this._reconnect();
        }
      } else if (error.code == Kafka.CODES.ERRORS.ERR__PARTITION_EOF) {
        this.emit('info', 'Received ERR__PARTITION_EOF', error);
      } else {
        const verror = new VError(error, 'Unexpected error on KafkaConsumer');
        this.emit('error', verror);
      }
    });
    producer.on('event.throttle', (throttle) => {
      this.emit('info', 'Received throttling event from the KafkaProducer', throttle);
    });
    producer.on('disconnected', () => {
      this._initialized = false;
      this.emit('info', 'Disconnected the KafkaProducer', arguments);
    });
    producer.on('delivery-report', (error, report) => {
      this._processing = false;
      if (error) {
        this.emit('delivery-report-error', new VError({
          cause: error,
          info: { report }
        }, 'Failed to delivery the message'));
      } else {
        try {
          report.value = JSON.parse(report.value.toString());
        } catch (e) {
          // should We do something here ?
        }
        this.emit('delivery-report', 'Received delivery-report event from the KafkaProducer', report);
      }
    });
  }


  /**
   * Produces message on a topic
   * @param {string} topic
   * @param {Object} message
   * @param {number} partition [optional,default=random] the partition to send, otherwise it will be randomly sent
   * @throws {VError} Thrown when it fails to send message
   * @memberof Producer
   */
  async publish(topic, message, partition) {
    if (this._reconnecting) {
      throw new VError('The Producer is reconnecting');
    }

    if (!this._initialized) {
      throw new VError('The Producer is not initialized, invoke the init method');
    }
    if (this._disposing) {
      this.emit('warn', 'Not sending the message, because the disposed was called', { topic, message, partition });
      return;
    }

    if (message == null && message == undefined) {
      throw new VError('Message must not be empty');
    }
    if (partition == null || partition == undefined) {
      partition = -1;
    }
    if (typeof partition !== 'number') {
      throw new VError('The partition parameter must be a number');
    }
    
    this._processing = true;
    try {
      message = Buffer.from(JSON.stringify(message));

      this.producer.produce(topic, partition, message);
    } catch (error) {
      this.emit('warn', new VError({
        cause: error,
        info: { topic, message, attempt: this._publishAttempts }
      }, 'Failed to produce the message, retrying...'));

      if (this._publishAttempts > 0) {
        await sleep(1000 * this._publishAttempts);
        this._publishAttempts--;
        
        await this.publish(topic, message, partition);

        this.emit('info', 'Publish retry succeeded', { topic, message, attempt: this._publishAttempts });
        return;
      }

      throw new VError({
        cause: error,
        info: { message, topic }
      }, 'Failed to produce the message');
    } finally {
      this._publishAttempts = 5;
    }
  }


  /**
   * Flushes the producer
   * @param {number} [timeout=500]
   * @returns
   * @memberof Producer
   */
  _flush(timeout = 500) {
    return new Promise((resolve, reject) => {
      this.producer.flush(timeout, (error) => {
        if (error) {
          return reject(new VError(error, 'Failed to glush the internal rdkafka queue'));
        }
        resolve();
      });
    });
  }

  /**
   * Disposes the producer connection
   * @memberof Producer
   */
  dispose() {
    if (!this._initialized) {
      return;
    }
    
    this._disposing = true;
    
    return new Promise(async (resolve, reject) => {
      while (this._processing) {
        await sleep(500);
      }

      try {
        await this._flush();
      } catch (error) {
        this.emit('error', error);
      } finally {
        clearInterval(this.pollInterval);
        this._initialized = false;
        
        this.producer.disconnect((err, data) => {
          if (err) {
            return reject(new VError(err, 'Failed to dispose KafkaProducer connection'));
          }
          resolve(data);
        });
      }
    });
  }
}

module.exports = Producer;
