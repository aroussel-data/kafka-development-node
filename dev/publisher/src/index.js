const { Kafka } = require("kafkajs");
const { getKafkaConnectSettings, getKafkaTopicName } = require("./config");

const kafka = new Kafka(getKafkaConnectSettings());
const topicName = getKafkaTopicName();

const producer = kafka.producer();

const run = async () => {
  await producer.connect();

  await producer.send({
    topic: topicName,
    messages: [
      {
        value: `sent from ${process.env.PUBLISHER_NAME}`,
      },
    ],
  });
};

run().catch(console.error);

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.forEach(type => {
  process.on(type, async e => {
    try {
      console.log(`process.on ${type}`);
      console.error(e);
      await producer.disconnect();
      process.exit(0);
    } catch (_) {
      console.log("Error during exit, forcing exit");
      process.exit(1);
    }
  })
});

signalTraps.forEach(type => {
  process.once(type, async () => {
    try {
      console.log("Disconnecting producer");
      await producer.disconnect();
    } finally {
      console.log("Killing the process");
      process.kill(process.pid, type);
    }
  })
});
