const { Kafka } = require('kafkajs');
const { MongoClient } = require('mongodb');

const kafka = new Kafka({ clientId: 'rutasur-analytics', brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'analitica' });

const mongoUri = 'mongodb://localhost:27017/?directConnection=true';
const dbName = 'rutasur';
let mongoClient;

async function run() {
  mongoClient = new MongoClient(mongoUri);
  await mongoClient.connect();
  console.log('📊 Servicio de Analítica conectado a MongoDB.');

  await consumer.connect();
  await consumer.subscribe({ topic: 'envios.estados', fromBeginning: true });

  console.log('📈 Analítica escuchando eventos para actualizar métricas diarias...');

  await consumer.run({
    eachMessage: async ({ message }) => {
      const evento = JSON.parse(message.value.toString());
      const { zonaId, tipo, motivo, ocurridoEn } = evento;

      if (!zonaId) return;

      const db = mongoClient.db(dbName);
      const metricasCollection = db.collection('metricas_diarias');

      const fecha = new Date(ocurridoEn || Date.now()).toISOString().split('T')[0];
      const franjaHoraria = new Date(ocurridoEn || Date.now()).getHours() < 12 ? 'MAÑANA' : 'TARDE';

      // Actualización atómica de agregados según el tipo de evento
      let updateFields = {};
      if (tipo === 'ENTREGADO') {
        updateFields = { $inc: { totalEntregados: 1, totalIntentos: 1 } };
      } else if (tipo === 'ENTREGA_FALLIDA') {
        const campoMotivo = `motivos.${motivo || 'DESCONOCIDO'}`;
        updateFields = { 
          $inc: { 
            totalFallidos: 1, 
            totalIntentos: 1, 
            [campoMotivo]: 1 
          } 
        };
      }

      if (Object.keys(updateFields).length > 0) {
        await metricasCollection.updateOne(
          { fecha, zonaId, franjaHoraria },
          updateFields,
          { upsert: true }
        );
        console.log(`📊 [CU4] Métrica actualizada para Zona: ${zonaId} (${fecha} - ${franjaHoraria})`);
      }
    }
  });
}

run().catch(console.error);