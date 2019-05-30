'use strict';

const { Schema, model } = require('mongoose');

const schema = new Schema({
  userId: {
    type: String,
    required: true,
    unique: true
  },
  phone: {
    type: String,
    required: true
  },
  deviceId: {
    type: String,
    required: true
  },
  thresholdTemperature: {
    type: Number,
    required: true
  },
  thresholdHumidity: {
    type: Number,
    required: true
  }
}, {
  timestamps: true,
  strict: false
});

module.exports = model('User', schema);