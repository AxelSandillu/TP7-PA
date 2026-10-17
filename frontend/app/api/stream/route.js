import { MongoClient } from 'mongodb';

const uri = 'mongodb://localhost:27017/?directConnection=true';
const client = new MongoClient(uri);

export async function GET(request) {
  const encoder = new TextEncoder();

  const customStream = new ReadableStream({
    async start(controller) {
      try {
        await client.connect();
        const db = client.db('rutasur');
        const collection = db.collection('envios');

        // Escuchar cambios en la colección de envíos (Change Stream)
        const changeStream = collection.watch();

        changeStream.on('change', (change) => {
          if (
            change.operationType === 'insert' ||
            change.operationType === 'update'
          ) {
            const updatedDoc = change.fullDocument;
            const data = JSON.stringify({
              type: 'UPDATE',
              payload: updatedDoc,
            });
            controller.enqueue(encoder.encode(`data: ${data}\n\n`));
          }
        });

        // Mantener la conexión viva con un ping cada 15 segundos
        const interval = setInterval(() => {
          controller.enqueue(
            encoder.encode(`data: ${JSON.stringify({ type: 'PING' })}\n\n`),
          );
        }, 15000);

        request.signal.addEventListener('abort', () => {
          clearInterval(interval);
          changeStream.close();
          client.close();
        });
      } catch (err) {
        console.error('Error en Change Stream:', err);
        controller.close();
      }
    },
  });

  return new Response(customStream, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
    },
  });
}
