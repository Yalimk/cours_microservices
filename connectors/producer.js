import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'producer',
  brokers: ['localhost:9092']
});

const producer = kafka.producer({
  maxInFlightRequests: 1,
  idempotent: true
});

const myProducer = async () => {
  try {
    await producer.connect();
    setInterval(async () => {
      console.log('données envoyées au topic1')
      await producer.send({
        topic: 'topic1',
        messages: [
          {value: 'Coucou'}
        ]
      })
    }, 3000);
  } catch (error) {
    console.error(error);
  }
};

await myProducer();