import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'producer-microservice',
  brokers: ['localhost:8888']
});

const producer = kafka.producer({
  maxInFlightRequests: 1,
  idempotent: true
});

// Production d'une donnée dans un topic
const writeDataToTopic = async (message, topicName) => {
  await producer.connect();
  await producer.send({
    topic: topicName,
    messages: [
      { value: message } //The value passed to 'value' has to be a string
    ]
  });
};

writeDataToTopic('Hello', 'topic1')