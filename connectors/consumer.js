import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'consumer',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({
  groupId: 'consumer'
});

const myConsumer = async () => {
  try {
    await consumer.connect();
    await consumer.subscribe({
      topic: 'topic2'
    });
    await consumer.run({
      eachMessage: async ({ message }) => {
        const receivedMessage = message.value.toString();
        console.log('Message reçu du topic2: ', receivedMessage);
      }
    })
  } catch (error) {
    console.error(error);
  }
};
await myConsumer();