import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'transformer-microservice',
  brokers: ['localhost:8888']
});

// Création du producer
const producer = kafka.producer({
  maxInFlightRequests: 1,
  idempotent: true
});

// Création du consumer
const consumer = kafka.consumer({
  groupId: 'transformer'
});

// Ecriture d'une donnée dans un topic
const writeDataToTopic = async (message, topicName) => {
  await producer.connect();
  await producer.send({
    topic: topicName,
    messages: [
      { value: message } //The value passed to 'value' has to be a string
    ]
  });
};

// Lecture et transformation des données d'un topic
const consumeAndTransformData = async (addedMessage, topicToReadFrom, topicToWriteTo) => {
  await consumer.connect();
  await consumer.subscribe({
    topic: topicToReadFrom,
  });
  await consumer.run({
    eachMessage: async ({ message }) => {
      const topicData = message.value.toString();
      const messageToSend = `${topicData} ${addedMessage}`;
      await writeDataToTopic(messageToSend, topicToWriteTo);
    }
  });
};

consumeAndTransformData('world!', 'topic1', 'topic2');