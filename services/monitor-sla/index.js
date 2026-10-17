const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'rutasur-monitor-sla',
  brokers: ['localhost:9092'],
});
const consumer = kafka.consumer({ groupId: 'monitor-sla' });
const producer = kafka.producer();

// Memoria interna para el monitoreo de estados y vencimientos de cada envío
const enviosMonitoreados = new Map();

async function run() {
  await producer.connect();
  await consumer.connect();

  // Suscribirse a los topics necesarios para construir el estado interno
  await consumer.subscribe({
    topics: ['envios.estados', 'envios.promesas'],
    fromBeginning: true,
  });

  console.log('⏱️ Monitor de SLA iniciado y evaluando riesgos...');

  // Evaluación periódica cada 10 segundos para verificar los SLAs en tiempo simulado
  setInterval(() => {
    const ahora = Date.now();

    for (const [shipmentId, data] of enviosMonitoreados.entries()) {
      if (!data.venceEn) continue;

      const tiempoRestante = new Date(data.venceEn).getTime() - ahora;
      const horasRestantes = tiempoRestante / (1000 * 60 * 60);

      let alertaPrioridad = null;

      // Reglas de SLA definidas en el TP[cite: 9]
      if (tiempoRestante < 0 && data.estadoActual !== 'ENTREGADO') {
        alertaPrioridad = 'CRITICA'; // Vencido
      } else if (
        horasRestantes <= 4 &&
        data.estadoActual !== 'EN_REPARTO' &&
        data.estadoActual !== 'ENTREGADO'
      ) {
        alertaPrioridad = 'ALTA'; // Sin salida
      } else if (horasRestantes <= 10 && data.estadoActual === 'CREADO') {
        alertaPrioridad = 'MEDIA'; // Sin ingreso al hub
      }

      if (alertaPrioridad && !data.alertaEmitida) {
        data.alertaEmitida = alertaPrioridad;
        console.log(
          `🚨 [CU3 - SLA] Alerta ${alertaPrioridad} emitida para envío ${shipmentId} (Vence: ${data.venceEn})`,
        );

        // Emitir evento de alerta de SLA
        producer
          .send({
            topic: 'alertas.sla',
            messages: [
              {
                key: shipmentId,
                value: JSON.stringify({
                  shipmentId,
                  prioridad: alertaPrioridad,
                  estadoActual: data.estadoActual,
                  generadoEn: new Date().toISOString(),
                }),
              },
            ],
          })
          .catch((err) => console.error('Error enviando alerta SLA:', err));
      }
    }
  }, 10000);

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      const rawValue = message.value.toString();
      const payload = JSON.parse(rawValue);
      const shipmentId = payload.shipmentId;

      if (!shipmentId) return;

      if (!enviosMonitoreados.has(shipmentId)) {
        enviosMonitoreados.set(shipmentId, {
          estadoActual: 'CREADO',
          venceEn: null,
          alertaEmitida: null,
        });
      }

      const envio = enviosMonitoreados.get(shipmentId);

      if (topic === 'envios.promesas') {
        envio.venceEn = payload.venceEn;
      } else if (topic === 'envios.estados') {
        envio.estadoActual = payload.tipo;

        // Si se entrega, se cierra la alerta si existía
        if (payload.tipo === 'ENTREGADO') {
          envio.alertaEmitida = 'CERRADA';
        }
      }
    },
  });
}

run().catch(console.error);
