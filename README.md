# RutaSur — Plataforma de Seguimiento Logístico en Tiempo Real

Sistema **event-driven** para el seguimiento de envíos de una empresa de logística. Los eventos de los envíos se publican en **Apache Kafka**, se proyectan en **MongoDB** y se consumen por distintos microservicios que reaccionan a fallos, monitorean SLAs, notifican clientes y generan métricas. Una torre de control en **Next.js** muestra el estado de los envíos en vivo vía Server-Sent Events.

## Arquitectura

```
   ┌────────────┐  envios.promesas / envios.estados
   │ simulator  │ ────────────────────────┐
   └────────────┘                         │
         │                                ▼
         │                         ┌──────────────┐
         │                         │    Kafka     │
         │                         └──────┬───────┘
         │                                │
         │        ┌───────────────────────┼────────────────────────┐
         │        ▼                       ▼                        ▼
         │  ┌──────────┐        ┌───────────────────┐     ┌────────────────┐
         │  │ projector│        │ services/analytics│     │ services/...   │
         │  └────┬─────┘        └─────────┬─────────┘     └───────┬────────┘
         │       │                        │                       │
         ▼       ▼                        ▼                       ▼
   ┌─────────────────────────────────────────┐          ┌──────────────────┐
   │                MongoDB (rutasur)        │          │  Topics de salida│
   │  envios · eventos_procesados · métricas │          │  alertas.sla ... │
   └────────────────────┬────────────────────┘          └──────────────────┘
                        │ Change Stream
                        ▼
                 ┌──────────────┐   SSE    ┌──────────────┐
                 │ /api/stream  │ ───────► │   frontend   │
                 └──────────────┘          └──────────────┘
```

## Componentes

| Componente | Descripción | Topics que consume | Topics que produce |
| --- | --- | --- | --- |
| `simulator` | Genera envíos simulados con su ciclo de vida completo cada 10 s. | — | `envios.promesas`, `envios.estados` |
| `projector` | Proyecta los estados en MongoDB aplicando **idempotencia** y **ordenamiento por secuencia**. Envía eventos inválidos a la **DLQ**. | `envios.estados` | `envios.estados.dlq` |
| `services/reprogramming` | **CU2** — Ante `ENTREGA_FALLIDA` decide la acción de reprogramación según el motivo e intentos. | `envios.estados` | `envios.reprogramaciones` |
| `services/notifications` | **CU2** — Notifica al cliente cuando hay una entrega fallida. | `envios.estados` | `notificaciones.enviadas` |
| `services/monitor-sla` | **CU3** — Evalúa vencimientos y emite alertas (`MEDIA`, `ALTA`, `CRITICA`). | `envios.estados`, `envios.promesas` | `alertas.sla` |
| `services/analytics` | **CU4** — Agrega métricas diarias por zona y franja horaria. | `envios.estados` | — (escribe en MongoDB) |
| `frontend` | Torre de control en Next.js. Refleja los envíos en tiempo real por SSE. | — | — |

### Topics de Kafka

- `envios.promesas` — promesa de entrega (`venceEn`).
- `envios.estados` — ciclo de vida del envío (`CREADO`, `INGRESADO_HUB`, `CLASIFICADO`, `EN_REPARTO`, `ENTREGADO`, `ENTREGA_FALLIDA`).
- `envios.estados.dlq` — eventos malformados o con esquema inválido.
- `envios.reprogramaciones` — acciones de reprogramación resultantes.
- `notificaciones.enviadas` — notificaciones enviadas al cliente.
- `alertas.sla` — alertas de riesgo de incumplimiento de SLA.

## Requisitos previos

- [Docker](https://www.docker.com/) y Docker Compose
- [Node.js](https://nodejs.org/) 18+ (probado con versiones modernas de npm)

## Puesta en marcha

### 1. Levantar la infraestructura (Kafka + MongoDB)

MongoDB se levanta como **replica set** (`rs0`), requisito para usar Change Streams.

```bash
docker compose up -d
```

### 2. Instalar dependencias

Cada componente es un proyecto Node independiente:

```bash
(cd simulator             && npm install)
(cd projector             && npm install)
(cd services/analytics    && npm install)
(cd services/monitor-sla  && npm install)
(cd services/notifications && npm install)
(cd services/reprogramming && npm install)
(cd frontend              && npm install)
```

### 3. Arrancar los procesos

Abre una terminal por cada uno de estos comandos (el orden no es crítico):

```bash
node simulator/simulator.js
node projector/projector.js
node services/reprogramming/index.js
node services/notifications/index.js
node services/monitor-sla/index.js
node services/analytics/index.js
```

Finalmente, la torre de control:

```bash
(cd frontend && npm run dev)
```

Abre [http://localhost:3000](http://localhost:3000) para ver la **Torre de Control**.

## Puertos

| Servicio | Puerto |
| --- | --- |
| Kafka (host) | `9092` |
| MongoDB | `27017` |
| Frontend (Next.js) | `3000` |

## Colecciones de MongoDB (`rutasur`)

- `envios` — estado actual e historial de cada envío (documento proyectado).
- `eventos_procesados` — registro de idempotencia por consumidor.
- `metricas_diarias` — agregados de entregas/fallos por zona y franja horaria.

## Notas

- Los brokers de Kafka se asumen en `localhost:9092` y MongoDB en `mongodb://localhost:27017` (`directConnection=true`).
- El frontend consume los cambios de `envios` mediante un **Change Stream** expuesto como SSE en `frontend/app/api/stream/route.js`.
- La configuración de infraestructura está en `docker-compose.yml` en la raíz del repositorio.
