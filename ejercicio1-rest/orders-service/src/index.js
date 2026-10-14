'use strict';

const http = require('node:http');
const { randomUUID } = require('node:crypto');
const { sendJson, readJson, createRouter, httpError } = require('./http-utils');
const { createInventoryClient, withRetry } = require('./inventory-client');

const PORT = Number(process.env.PORT || 3000);
const RETRIES = Number(process.env.INVENTORY_RETRIES || 2);

const inventory = createInventoryClient();
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
  sendJson(res, 200, { status: 'ok', service: 'orders-service', inventoryProtocol: inventory.protocol }),
);

router.get('/orders', ({ res }) => sendJson(res, 200, [...orders.values()]));

router.get('/orders/:id', ({ res, params }) => {
  const order = orders.get(params.id);
  if (!order) throw httpError(404, 'Pedido no encontrado');
  sendJson(res, 200, order);
});

// Crear pedido: llamada SÍNCRONA a InventoryService antes de confirmar.
router.post('/orders', async ({ req, res }) => {
  const input = validateOrder(await readJson(req));

  const started = performance.now();
  let availability;
  try {
    availability = await withRetry(() => inventory.checkAvailability(input.productId, input.quantity), {
      retries: RETRIES,
    });
  } catch (err) {
    if (err.code === 'NOT_FOUND') throw httpError(404, err.message);
    // Acoplamiento temporal: si Inventory no responde, no podemos confirmar el pedido.
    console.error(`[orders] No se pudo verificar stock: ${err.message}`);
    return sendJson(res, 503, {
      error: 'No se pudo verificar el stock: InventoryService no disponible. Intente más tarde.',
      detail: err.message,
    });
  }
  const latencyMs = Math.round((performance.now() - started) * 100) / 100;

  const order = {
    id: randomUUID(),
    ...input,
    productName: availability.name,
    status: availability.available ? 'CONFIRMADO' : 'RECHAZADO',
    reason: availability.available ? undefined : `Stock insuficiente (disponible: ${availability.stock})`,
    inventoryCheck: { protocol: inventory.protocol, latencyMs, stock: availability.stock },
    createdAt: new Date().toISOString(),
  };
  orders.set(order.id, order);
  console.log(`[orders] Pedido ${order.id} ${order.status} (${input.productId} x${input.quantity}, ${latencyMs} ms vía ${inventory.protocol})`);
  sendJson(res, availability.available ? 201 : 409, order);
});

http
  .createServer(router.handle)
  .listen(PORT, () => console.log(`OrdersService escuchando en http://0.0.0.0:${PORT} (inventario vía ${inventory.protocol.toUpperCase()})`));
