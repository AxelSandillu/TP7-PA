const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'rutasur-notifications',
  brokers: ['localhost:9092'],
});
const consumer = kafka.consumer({ groupId: 'notificaciones' });
const producer = kafka.producer();

async function run() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: 'envios.estados', fromBeginning: true });

  console.log('🔔 Servicio de Notificaciones escuchando fallos...');

  await consumer.run({
    eachMessage: async ({ message }) => {
      const evento = JSON.parse(message.value.toString());

      if (evento.tipo === 'ENTREGA_FALLIDA') {
        const { shipmentId, moti_detalle } = evento;
        console.log(
          `✉️ [CU2 - Notificaciones] Enviando aviso al cliente por envío ${shipmentId} (Motivo: ${evento.motivo || 'FALLO'})`,
        );

        // Emitir evento de notificación enviada
        await producer.send({
          topic: 'notificaciones.enviadas',
          messages: [
            {
              key: shipmentId,
              value: JSON.stringify({
                shipmentId,
                enviadoEn: new Date().toISOString(),
                estadoNotificacion: 'ENVIADO',
              }),
            },
          ],
        });
      }
    },
  });
}

run().catch(console.error);
