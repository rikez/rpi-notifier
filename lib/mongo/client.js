'use strict';

const EventEmitter = require('events');
const mongoose = require('mongoose');
const VError = require('verror');

/**
 * MongoDB wrapper.
 *
 * @class MongoClient
 * @extends {EventEmitter}
 */
class MongoClient extends EventEmitter {
  /**
   * Creates an instance of MongoClient.
   * 
   * @param {string} mongoURI MongoDB connection URI.
   * @param {*} mongoOpts Mongoose connection options.
   * @memberof MongoClient
   */
  constructor(mongoURI, mongoOpts) {
    super();
    
    this._URI = mongoURI;
    this._options = mongoOpts;    
  }

  /**
   * Return MongoDB connection.
   *
   * @readonly
   * @memberof MongoClient
   */
  get connection() {
    return mongoose.connection;
  }

  /**
   * Listen events.
   * Start connection.
   *
   * @memberof MongoClient
   */
  async init() {
    this._listenEvents();
    await this._createConnection();
  }

  /**
   * Close mongo connection pool.
   *
   * @memberof MongoClient
   */
  async dispose() {
    // Close connection
    await mongoose.connection.close();

    // Remove listener.
    mongoose.connection.removeListener('connected', this._connectedListener);
    mongoose.connection.removeListener('reconnected', this._reconnectedListener);
    mongoose.connection.removeListener('disconnected', this._disconnectedListener);
    mongoose.connection.removeListener('error', this._errorListener);
  }

  /**
   * Create mongo conenction with URI and Options.
   *
   * @returns Promise.
   * @memberof MongoClient
   */
  _createConnection() {
    return new Promise((resolve, reject) => {
      mongoose.connect(this._URI, this._options, (err) => {
        if (err) {
          reject(err);
        }

        resolve();
      });
    });
  }

  /**
   * Listen connection events.
   *
   * @memberof MongoClient
   */
  _listenEvents() {  
    // Save listeners.
    this._connectedListener = () => this.emit('info', 'Mongo connection established.');
    this._disconnectedListener = () => {
      this.emit('warn', 'Mongo connection lost.');
    };
    this._reconnectedListener = () => this.emit('info', 'Mongo reconnected successfuly.');
    this._errorListener = (err) => {
      const verror = new VError(err, 'Mongo connection error');
      this.emit('error', verror);
    };

    // Connect event.
    mongoose.connection.on('connected', this._connectedListener);

    // Disconnect event (Connection close, Network errir, Server crashes).
    mongoose.connection.on('disconnected', this._disconnectedListener);

    // Reconnect event.
    mongoose.connection.on('reconnected', this._reconnectedListener);

    // Connection error ocurred.
    mongoose.connection.on('error', this._errorListener);
  }
}

module.exports = MongoClient;