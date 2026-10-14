'use strict';

const http = require('node:http');
const { randomUUID } = require('node:crypto');
const { sendJson, readJson, createRouter, httpError } = require('./http-utils');
const { createInventoryClient, withRetry } = require('./inventory-client');
const { EventPublisher } = require('./event-publisher');
const { ROUTING_KEY } = require('./topology');

const PORT = Number(process.env.PORT || 3000);
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://admin:admin@localhost:5672';

const inventory = createInventoryClient();
const publisher = new EventPublisher(RABBITMQ_URL);
const orders = new Map();
const router = createRouter();

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
    rabbitmq: publisher.connected ? 'conectado' : 'desconectado',
    pendingEvents: publisher.pending.length,
  }),
);

router.get('/orders', ({ res }) => sendJson(res, 200, [...orders.values()]));

router.get('/orders/:id', ({ res, params }) => {
  const order = orders.get(params.id);
  if (!order) throw httpError(404, 'Pedido no encontrado');
  sendJson(res, 200, order);
});

router.post('/orders', async ({ req, res }) => {
  const input = validateOrder(await readJson(req));

  // 1) Verificación de stock SÍNCRONA (igual que en el ejercicio 1).
  let availability;
  try {
    availability = await withRetry(() => inventory.checkAvailability(input.productId, input.quantity));
  } catch (err) {
    if (err.code === 'NOT_FOUND') throw httpError(404, err.message);
    return sendJson(res, 503, { error: 'InventoryService no disponible', detail: err.message });
  }
  if (!availability.available) {
    throw httpError(409, `Stock insuficiente de ${input.productId} (disponible: ${availability.stock})`);
  }

  const order = {
    id: randomUUID(),
    ...input,
    productName: availability.name,
    status: 'CREADO',
    createdAt: new Date().toISOString(),
  };
  orders.set(order.id, order);

  // 2) Notificación ASÍNCRONA: publicamos el evento y respondemos sin esperar el envío del mail.
  const event = {
    eventId: randomUUID(),
    eventType: 'PedidoCreado',
    occurredAt: order.createdAt,
    orderId: order.id,
    data: {
      customerEmail: order.customerEmail,
      productId: order.productId,
      productName: order.productName,
      quantity: order.quantity,
    },
  };
  const published = await publisher.publish(ROUTING_KEY, event);

  console.log(`[orders] Pedido ${order.id} creado (evento ${published ? 'publicado' : 'pendiente de envío'})`);
  sendJson(res, 201, { ...order, eventPublished: published });
});

publisher.start();
http.createServer(router.handle).listen(PORT, () => console.log(`OrdersService escuchando en http://0.0.0.0:${PORT}`));
