# rpi-notifer

The rpi-notifier is a microservice responsible for the notification of metrics. It consumes messages from the Kafka, verifies the user's device ownership, performs a dialing, and finally publishes a new on message on a topic that will plot graphs on Kibana.

## Running locally

```
 npm install

 npm start
```

## Docker Container

```
  docker build . -t rpi-notifier
  docker run --env-file .env --restart unless-stopped --memory="2g" --cpus="2.0" -d rpi-notifier
```