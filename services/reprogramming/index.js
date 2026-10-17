const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'rutasur-reprogramming',
  brokers: ['localhost:9092'],
});
const consumer = kafka.consumer({ groupId: 'reprogramacion' });
const producer = kafka.producer();

async function run() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: 'envios.estados', fromBeginning: true });

  console.log('🔄 Servicio de Reprogramación escuchando fallos...');

  await consumer.run({
    eachMessage: async ({ message }) => {
      const evento = JSON.parse(message.value.toString());

      if (evento.tipo === 'ENTREGA_FALLIDA') {
        const { shipmentId, motivo, intentoActual } = evento;
        let nuevaAccion = 'REPROGRAMADO';

        // Reglas de reprogramación especificadas en el TP
        if (intentoActual >= 3 || motivo === 'RECHAZADO') {
          nuevaAccion = 'DEVOLUCION_COMERCIO';
        } else if (motivo === 'DIRECCION_INCORRECTA') {
          nuevaAccion = 'RETENIDO';
        } else if (motivo === 'AUSENTE') {
          nuevaAccion = 'REPROGRAMADO_SIGUIENTE_DIA';
        }

        console.log(
          `🔄 [CU2 - Reprogramación] Envío ${shipmentId} con motivo ${motivo} -> Acción tomada: ${nuevaAccion}`,
        );

        await producer.send({
          topic: 'envios.reprogramaciones',
          messages: [
            {
              key: shipmentId,
              value: JSON.stringify({
                shipmentId,
                accion: nuevaAccion,
                reprogramadoEn: new Date().toISOString(),
              }),
            },
          ],
        });
      }
    },
  });
}

run().catch(console.error);
