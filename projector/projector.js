const { Kafka } = require('kafkajs');
const { MongoClient } = require('mongodb');

// Configuración de Kafka
const kafka = new Kafka({
  clientId: 'rutasur-projector',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'proyector' });
const producer = kafka.producer();

const mongoUri = 'mongodb://localhost:27017/?directConnection=true';
const dbName = 'rutasur';

async function run() {
  await mongoClientConnect();
  await producer.connect();
  await consumer.connect();

  await consumer.subscribe({ topic: 'envios.estados', fromBeginning: true });
  console.log('👁️ Proyector conectado y escuchando el topic: envios.estados');

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const rawValue = message.value.toString();
      let evento;

      try {
        evento = JSON.parse(rawValue);
      } catch (err) {
        console.error('❌ Error al parsear JSON, enviando a DLQ:', rawValue);
        await enviarADlq(rawValue, 'JSON_MALFORMADO');
        return;
      }

      if (
        !evento.eventId ||
        !evento.shipmentId ||
        !evento.tipo ||
        evento.secuencia === undefined
      ) {
        console.warn('⚠️ Evento con esquema inválido, enviando a DLQ:', evento);
        await enviarADlq(evento, 'ESQUEMA_INVALIDO');
        return;
      }

      const { shipmentId, eventId, tipo, secuencia, ocurridoEn } = evento;

      const db = mongoClient.db(dbName);
      const enviosCollection = db.collection('envios');
      const eventosProcesadosCollection = db.collection('eventos_procesados');

      const yaProcesado = await eventosProcesadosCollection.findOne({
        consumidor: 'proyector',
        eventId,
      });
      if (yaProcesado) {
        console.log(
          `ℹ️ Evento duplicado descartado: ${eventId} para envío ${shipmentId}`,
        );
        return;
      }

      const envioActual = await enviosCollection.findOne({ shipmentId });
      if (envioActual && secuencia <= envioActual.ultimaSecuencia) {
        console.log(`⚠️ Evento fuera de orden descartado para ${shipmentId}`);
        return;
      }

      await enviosCollection.updateOne(
        { shipmentId },
        {
          $set: {
            shipmentId,
            estadoActual: tipo,
            ultimaSecuencia: secuencia,
            hubId: evento.hubId || null,
            zonaId: evento.zonaId || null,
            repartidorId: evento.repartidorId || null,
            actualizadoEn: new Date(),
          },
          $push: {
            historial: {
              eventId,
              tipo,
              secuencia,
              ocurridoEn: ocurridoEn || new Date().toISOString(),
            },
          },
        },
        { upsert: true },
      );

      await eventosProcesadosCollection.insertOne({
        consumidor: 'proyector',
        eventId,
        procesadoEn: new Date(),
      });

      console.log(
        `📥 [CU1] Envío ${shipmentId} actualizado a estado: ${tipo} (Secuencia: ${secuencia})`,
      );
    },
  });
}

async function enviarADlq(payload, motivo) {
  try {
    await producer.send({
      topic: 'envios.estados.dlq',
      messages: [
        {
          value: JSON.stringify({
            payload,
            motivo,
            registradoEn: new Date().toISOString(),
          }),
        },
      ],
    });
    console.log(`📤 Evento redirigido a DLQ por motivo: ${motivo}`);
  } catch (err) {
    console.error('❌ Error crítico al enviar a DLQ:', err);
  }
}

let mongoClient;
async function mongoClientConnect() {
  mongoClient = new MongoClient(mongoUri);
  await mongoClient.connect();
  console.log('📦 Proyector conectado a MongoDB exitosamente.');
}

run().catch(console.error);
