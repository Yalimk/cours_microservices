import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'transformer',
  brokers: ['localhost:9092']
});

const producer = kafka.producer({
  maxInFlightRequests: 1,
  idempotent: true
});
try {
  await producer.connect();
} catch (error) {
  console.error('erreur de producer.connect outside function: ', error);
};

const consumer = kafka.consumer({
  groupId: 'transformer-consumer'
});

const myProducerInTransformer = async (messageToSend) => {
  try {
    console.log('données envoyées au topic2')
    await producer.send({
      topic: 'topic2',
      messages: [
        { value: messageToSend }
      ]
    })
  } catch (error) {
    console.error(error);
  }
};

const myTransformer = async (addedMessage) => {
  try {
    await consumer.connect();
    await consumer.subscribe({
      topic: 'topic1'
    });
    await consumer.run({
      eachMessage: async ({ message }) => {
        let messageToSend;
        const receivedMessage = message.value.toString();
        messageToSend = `${receivedMessage} ${addedMessage}`;
        try {
          await myProducerInTransformer(messageToSend);
        } catch (error) {
          console.error(error);
        };
      }
    })
  } catch (error) {
    console.error(error);
  }
};

myTransformer('les amis !');