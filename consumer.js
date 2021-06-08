import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'consumer-microservice',
  brokers: ['localhost:8888']
});

const consumer = kafka.consumer({
  groupId: 'consumer-2'
});

// Lecture des données d'un topic
const consumeData = async (topicToReadFrom) => {
  await consumer.connect();
  await consumer.subscribe({
    topic: topicToReadFrom,
  });
  await consumer.run({
    eachMessage: async ({ message }) => {
      console.log('Données contenues dans le topic 2: ', message.value.toString());
    }
  });
};

consumeData('topic2');