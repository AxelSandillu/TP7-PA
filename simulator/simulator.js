const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'rutasur-simulator',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();

const ZONAS = ['ZONA_NORTE', 'ZONA_SUR', 'ZONA_ESTE', 'ZONA_OESTE'];
const HUBS = ['HUB_1', 'HUB_2', 'HUB_3'];
const MOTIVOS_FALLO = [
  'AUSENTE',
  'DIRECCION_INCORRECTA',
  'RECHAZADO',
  'ZONA_INACCESIBLE',
];

function getRandomItem(array) {
  return array[Math.floor(Math.random() * array.length)];
}

async function run() {
  await producer.connect();
  console.log('🚀 Simulador de RutaSur conectado a Kafka.');
  console.log(
    '📦 Generando envíos controlados (1 cada 10 segundos)... Presiona Ctrl+C para detener.\n',
  );

  // Generar un envío cada 10 segundos para no saturar la terminal ni los topics
  setInterval(async () => {
    const shipmentId = 'ENV-' + Math.floor(100000 + Math.random() * 900000);
    const hubId = getRandomItem(HUBS);
    const zonaId = getRandomItem(ZONAS);
    const repartidorId = 'REP-' + Math.floor(10 + Math.random() * 90);
    const now = new Date().toISOString();

    // 1. Emitir Promesa de Entrega (Topic: envios.promesas)[cite: 3, 4]
    const promesaVencimiento = new Date(
      Date.now() + 24 * 60 * 60 * 1000,
    ).toISOString();
    await producer.send({
      topic: 'envios.promesas',
      messages: [
        {
          key: shipmentId,
          value: JSON.stringify({
            shipmentId,
            venceEn: promesaVencimiento,
            version: '1.0',
          }),
        },
      ],
    });

    // Secuencia de estados del ciclo de vida (CU1)[cite: 4, 6]
    const estadosSecuencia = [
      'CREADO',
      'INGRESADO_HUB',
      'CLASIFICADO',
      'EN_REPARTO',
      'ENTREGADO',
    ];

    for (let i = 0; i < estadosSecuencia.length; i++) {
      const tipoEstado = estadosSecuencia[i];

      await producer.send({
        topic: 'envios.estados',
        messages: [
          {
            key: shipmentId,
            value: JSON.stringify({
              eventId: `EVT-${shipmentId}-${i + 1}`,
              shipmentId,
              tipo: tipoEstado,
              ocurridoEn: new Date().toISOString(),
              secuencia: i + 1,
              hubId,
              zonaId,
              repartidorId,
              version: '1.0',
            }),
          },
        ],
      });

      // Pequeña pausa simulada entre estados de un mismo paquete
      await new Promise((resolve) => setTimeout(resolve, 1500));
    }

    console.log(`✅ Envío ${shipmentId} completó su ciclo feliz.`);
  }, 10000); // Cada 10 segundos se crea un nuevo envío completo
}

run().catch(console.error);
