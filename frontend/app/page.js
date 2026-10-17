'use client';

import { useState, useEffect } from 'react';

export default function TorreDeControl() {
  const [envios, setEnvios] = useState([]);
  const [conectado, setConectado] = useState(false);

  useEffect(() => {
    // Conectar a los Server-Sent Events (SSE)
    const eventSource = new EventSource('/api/stream');

    eventSource.onopen = () => setConectado(true);

    eventSource.onmessage = (event) => {
      const data = JSON.parse(event.data);
      if (data.type === 'UPDATE' && data.payload) {
        setEnvios((prev) => {
          const index = prev.findIndex(
            (e) => e && e.shipmentId === data.payload.shipmentId,
          );
          if (index >= 0) {
            const copia = [...prev];
            copia[index] = data.payload;
            return copia;
          } else {
            return [data.payload, ...prev].slice(0, 20); // Mostrar últimos 20
          }
        });
      }
    };

    eventSource.onerror = () => {
      setConectado(false);
      eventSource.close();
    };

    return () => eventSource.close();
  }, []);

  return (
    <main className="min-h-screen bg-gray-900 text-white p-8">
      <div className="max-w-6xl mx-auto">
        <header className="flex justify-between items-center mb-8 border-b border-gray-700 pb-4">
          <div>
            <h1 className="text-3xl font-bold">RutaSur - Torre de Control</h1>
            <p className="text-gray-400">
              Plataforma de seguimiento logístico en tiempo real
            </p>
          </div>
          <div className="flex items-center gap-2">
            <span
              className={`h-3 w-3 rounded-full ${conectado ? 'bg-green-500 animate-pulse' : 'bg-red-500'}`}
            ></span>
            <span className="text-sm text-gray-300">
              {conectado ? 'Conectado (En vivo)' : 'Desconectado'}
            </span>
          </div>
        </header>

        <section className="bg-gray-800 rounded-lg p-6 shadow-lg border border-gray-700">
          <h2 className="text-xl font-semibold mb-4">
            Últimos Envíos en Operación
          </h2>

          {envios.filter(Boolean).length === 0 ? (
            <p className="text-gray-400 text-center py-8">
              Esperando eventos en tiempo real desde MongoDB...
            </p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-left border-collapse">
                <thead>
                  <tr className="border-b border-gray-700 text-gray-400 text-sm">
                    <th className="p-3">ID de Envío</th>
                    <th className="p-3">Estado Actual</th>
                    <th className="p-3">Zona</th>
                    <th className="p-3">Hub</th>
                    <th className="p-3">Secuencia</th>
                    <th className="p-3">Última Actualización</th>
                  </tr>
                </thead>
                <tbody>
                  {envios.filter(Boolean).map((envio, index) => (
                    <tr
                      key={envio.shipmentId || index}
                      className="border-b border-gray-700/50 hover:bg-gray-700/30"
                    >
                      <td className="p-3 font-mono text-cyan-400">
                        {envio.shipmentId || 'N/A'}
                      </td>
                      <td className="p-3">
                        <span
                          className={`px-2 py-1 rounded text-xs font-bold ${
                            envio.estadoActual === 'ENTREGADO'
                              ? 'bg-green-900 text-green-300'
                              : envio.estadoActual === 'ENTREGA_FALLIDA'
                                ? 'bg-red-900 text-red-300'
                                : 'bg-blue-900 text-blue-300'
                          }`}
                        >
                          {envio.estadoActual || 'DESCONOCIDO'}
                        </span>
                      </td>
                      <td className="p-3 text-gray-300">
                        {envio.zonaId || 'N/A'}
                      </td>
                      <td className="p-3 text-gray-300">
                        {envio.hubId || 'N/A'}
                      </td>
                      <td className="p-3 text-gray-300">
                        {envio.ultimaSecuencia ?? 'N/A'}
                      </td>
                      <td className="p-3 text-gray-400 text-sm">
                        {envio.actualizadoEn
                          ? new Date(envio.actualizadoEn).toLocaleTimeString()
                          : 'N/A'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      </div>
    </main>
  );
}
