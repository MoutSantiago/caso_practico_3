'use strict';

const http = require('node:http');
const { Kafka, logLevel } = require('kafkajs');
const { sendJson, createRouter, httpError } = require('./http-utils');

const PORT = Number(process.env.PORT || 3003);
const TOPIC = process.env.ORDER_EVENTS_TOPIC || 'order-events';
const GROUP_ID = process.env.KAFKA_GROUP_ID || 'analytics-service';

const kafka = new Kafka({
  clientId: 'analytics-service',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  logLevel: logLevel.WARN,
  retry: { initialRetryTime: 500, retries: 10 },
});

// autoCommit: false -> este grupo nunca guarda offsets. Cada vez que arranca, relee el topic
// DESDE EL PRINCIPIO y reconstruye sus métricas en memoria (event sourcing / replay).
// Esto es posible porque Kafka conserva los eventos (retención), cosa que RabbitMQ no hace.
const consumer = kafka.consumer({ groupId: GROUP_ID });

// ---------- Estado (proyección en memoria) ----------
const stats = {
  totalEvents: 0,
  eventsByType: {},
  eventsByPartition: {},
  unitsByProduct: {}, // unidades confirmadas por producto
  firstEventAt: null,
  lastEventAt: null,
};
const orders = new Map(); // orderId -> { status, productId, quantity, timeline: [] }

function apply(event, partition, offset) {
  stats.totalEvents++;
  stats.eventsByType[event.eventType] = (stats.eventsByType[event.eventType] || 0) + 1;
  stats.eventsByPartition[partition] = (stats.eventsByPartition[partition] || 0) + 1;
  stats.firstEventAt ??= event.occurredAt;
  stats.lastEventAt = event.occurredAt;

  const order = orders.get(event.orderId) || {
    orderId: event.orderId,
    productId: event.data.productId,
    quantity: event.data.quantity,
    partition,
    timeline: [],
  };
  order.status = event.status;
  order.timeline.push({ status: event.status, at: event.occurredAt, offset });
  orders.set(event.orderId, order);

  if (event.eventType === 'OrderConfirmed') {
    const p = event.data.productId;
    stats.unitsByProduct[p] = (stats.unitsByProduct[p] || 0) + event.data.quantity;
  }
}

const secondsBetween = (a, b) => (Date.parse(b) - Date.parse(a)) / 1000;

function summary() {
  const ordersByStatus = {};
  const createdToShipped = [];
  for (const o of orders.values()) {
    ordersByStatus[o.status] = (ordersByStatus[o.status] || 0) + 1;
    const created = o.timeline.find((t) => t.status === 'CREADO');
    const shipped = o.timeline.find((t) => t.status === 'ENVIADO');
    if (created && shipped) createdToShipped.push(secondsBetween(created.at, shipped.at));
  }
  const total = orders.size;
  const rejected = ordersByStatus.RECHAZADO || 0;
  const avg = createdToShipped.length
    ? +(createdToShipped.reduce((a, b) => a + b, 0) / createdToShipped.length).toFixed(2)
    : null;
  return {
    ...stats,
    totalOrders: total,
    ordersByStatus,
    rejectionRate: total ? +(rejected / total).toFixed(3) : 0,
    avgSecondsCreatedToShipped: avg,
  };
}

// ---------- API HTTP ----------
const router = createRouter();
router.get('/health', ({ res }) => sendJson(res, 200, { status: 'ok', service: 'analytics-service' }));
router.get('/stats', ({ res }) => sendJson(res, 200, summary()));
router.get('/stats/orders', ({ res }) => sendJson(res, 200, [...orders.values()]));
router.get('/stats/orders/:id', ({ res, params }) => {
  const order = orders.get(params.id);
  if (!order) throw httpError(404, 'Pedido sin eventos registrados');
  sendJson(res, 200, order);
});

async function main() {
  http.createServer(router.handle).listen(PORT, () => console.log(`AnalyticsService escuchando en http://0.0.0.0:${PORT}`));

  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ partition, message }) => {
      const event = JSON.parse(message.value.toString());
      apply(event, partition, message.offset);
      console.log(`[analytics] p${partition}@${message.offset} ${event.eventType} pedido ${event.orderId.slice(0, 8)} (total eventos: ${stats.totalEvents})`);
    },
  });
  console.log(`AnalyticsService consumiendo "${TOPIC}" desde el inicio como grupo "${GROUP_ID}"`);
}

consumer.on(consumer.events.CRASH, ({ payload }) => {
  console.error('[kafka] El consumidor falló:', payload.error.message);
  if (!payload.restart) process.exit(1);
});

const shutdown = async () => {
  await consumer.disconnect().catch(() => {});
  process.exit(0);
};
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

main().catch((err) => {
  console.error('Error fatal:', err.message);
  process.exit(1);
});
