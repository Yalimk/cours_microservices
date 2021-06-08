import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'microservices-test-app',
  brokers: ['localhost:9091', 'localhost:9092']
});

const producer = kafka.producer({
  maxInFlightRequests: 1,
  idempotent: true
});

const consumer = kafka.consumer({
  groupId: 'test-app-group'
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

// Lecture des données d'un topic
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
  await consumer.disconnect();
};

// La principale fonction qui utiliser les différents connecteurs
const startTheWholeThing = async () => {
  await writeDataToTopic('Hello', 'topic1');
  await consumeAndTransformData('World!', 'topic1', 'topic2');
  await consumeAndTransformData(' Holà', 'topic2', 'topic3');
  await consumeAndTransformData(' amigos!', 'topic3', 'topic4');
};

await startTheWholeThing();