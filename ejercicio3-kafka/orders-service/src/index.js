'use strict';

const http = require('node:http');
const { randomUUID } = require('node:crypto');
const { sendJson, readJson, createRouter, httpError } = require('./http-utils');
const { createInventoryClient, withRetry } = require('./inventory-client');
const { EventPublisher } = require('./event-publisher');

const PORT = Number(process.env.PORT || 3000);

const inventory = createInventoryClient();
const publisher = new EventPublisher({
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  topic: process.env.ORDER_EVENTS_TOPIC || 'order-events',
  partitions: Number(process.env.ORDER_EVENTS_PARTITIONS || 3),
});
const orders = new Map();
const router = createRouter();

// Máquina de estados del pedido:
//   CREADO -> CONFIRMADO -> ENVIADO
//   CREADO -> RECHAZADO (sin stock)
const EVENT_BY_STATUS = {
  CREADO: 'OrderCreated',
  CONFIRMADO: 'OrderConfirmed',
  RECHAZADO: 'OrderRejected',
  ENVIADO: 'OrderShipped',
};

// Cambia el estado y publica el evento correspondiente en Kafka.
async function transition(order, newStatus, extra = {}) {
  const previousStatus = order.status ?? null;
  const at = new Date().toISOString();
  order.status = newStatus;
  order.updatedAt = at;
  Object.assign(order, extra);
  order.history.push({ status: newStatus, at });

  const event = {
    eventId: randomUUID(),
    eventType: EVENT_BY_STATUS[newStatus],
    orderId: order.id,
    status: newStatus,
    previousStatus,
    occurredAt: at,
    data: {
      customerEmail: order.customerEmail,
      productId: order.productId,
      productName: order.productName,
      quantity: order.quantity,
      ...extra, // reason (rechazo) o trackingCode (envío)
    },
  };
  const published = await publisher.publish(event);
  order.lastEventPublished = published;
  console.log(`[orders] Pedido ${order.id.slice(0, 8)}: ${previousStatus ?? '-'} -> ${newStatus}`);
}

// Consulta stock (REST síncrono, como en el ejercicio 1) y confirma o rechaza.
async function confirmWithInventory(order) {
  const availability = await withRetry(() => inventory.checkAvailability(order.productId, order.quantity));
  if (availability.available) await transition(order, 'CONFIRMADO');
  else await transition(order, 'RECHAZADO', { reason: `Stock insuficiente (disponible: ${availability.stock})` });
}

function getOrder(id) {
  const order = orders.get(id);
  if (!order) throw httpError(404, 'Pedido no encontrado');
  return order;
}

function validateOrder(body) {
  const { productId, quantity, customerEmail } = body;
  if (typeof productId !== 'string' || !productId) throw httpError(400, 'productId es obligatorio');
  if (!Number.isInteger(quantity) || quantity <= 0) throw httpError(400, 'quantity debe ser un entero positivo');
  if (typeof customerEmail !== 'string' || !customerEmail.includes('@')) {
    throw httpError(400, 'customerEmail debe ser un email válido');
  }
  return { productId, quantity, customerEmail };
}

router.get('/health', ({ res }) =>
  sendJson(res, 200, {
    status: 'ok',
    service: 'orders-service',
    kafka: publisher.ready ? 'conectado' : 'desconectado',
    pendingEvents: publisher.pending.length,
  }),
);

router.get('/orders', ({ res }) => sendJson(res, 200, [...orders.values()]));
router.get('/orders/:id', ({ res, params }) => sendJson(res, 200, getOrder(params.id)));

// Crea el pedido (evento OrderCreated) e intenta confirmarlo contra el inventario
// (evento OrderConfirmed u OrderRejected).
router.post('/orders', async ({ req, res }) => {
  const input = validateOrder(await readJson(req));

  // Validamos que el producto exista antes de crear nada.
  let product;
  try {
    product = await withRetry(() => inventory.checkAvailability(input.productId, input.quantity));
  } catch (err) {
    if (err.code === 'NOT_FOUND') throw httpError(404, err.message);
    product = null; // Inventory caído: el pedido queda CREADO y se confirma después
  }

  const order = { id: randomUUID(), ...input, productName: product?.name ?? input.productId, status: null, history: [] };
  orders.set(order.id, order);
  await transition(order, 'CREADO');

  if (!product) {
    return sendJson(res, 202, {
      ...order,
      message: 'InventoryService no disponible: el pedido quedó CREADO. Confirmalo luego con POST /orders/:id/confirm',
    });
  }
  if (product.available) await transition(order, 'CONFIRMADO');
  else await transition(order, 'RECHAZADO', { reason: `Stock insuficiente (disponible: ${product.stock})` });
  sendJson(res, 201, order);
});

// Reintenta la confirmación de un pedido que quedó CREADO.
router.post('/orders/:id/confirm', async ({ res, params }) => {
  const order = getOrder(params.id);
  if (order.status !== 'CREADO') throw httpError(409, `No se puede confirmar un pedido en estado ${order.status}`);
  try {
    await confirmWithInventory(order);
  } catch (err) {
    return sendJson(res, 503, { error: 'InventoryService no disponible', detail: err.message });
  }
  sendJson(res, 200, order);
});

// Despacha el pedido (evento OrderShipped).
router.post('/orders/:id/ship', async ({ res, params }) => {
  const order = getOrder(params.id);
  if (order.status !== 'CONFIRMADO') throw httpError(409, `Sólo se envían pedidos CONFIRMADOS (estado actual: ${order.status})`);
  await transition(order, 'ENVIADO', { trackingCode: `AR${Math.floor(Math.random() * 1e9)}` });
  sendJson(res, 200, order);
});

publisher.start();
http.createServer(router.handle).listen(PORT, () => console.log(`OrdersService escuchando en http://0.0.0.0:${PORT}`));
