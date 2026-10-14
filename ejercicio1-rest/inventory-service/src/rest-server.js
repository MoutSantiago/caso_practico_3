'use strict';

const http = require('node:http');
const { sendJson, createRouter, httpError } = require('./http-utils');
const { products, checkAvailability } = require('./data');

const LATENCY_MS = Number(process.env.SIMULATED_LATENCY_MS || 0);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function startRestServer(port) {
  const router = createRouter();

  router.get('/health', ({ res }) => sendJson(res, 200, { status: 'ok', service: 'inventory-service' }));

  router.get('/inventory', ({ res }) => sendJson(res, 200, [...products.values()]));

  router.get('/inventory/:productId', ({ res, params }) => {
    const product = products.get(params.productId);
    if (!product) throw httpError(404, `Producto ${params.productId} no existe`);
    sendJson(res, 200, product);
  });

  // Endpoint que consulta OrdersService antes de confirmar un pedido.
  router.get('/inventory/:productId/availability', async ({ res, params, query }) => {
    if (LATENCY_MS) await sleep(LATENCY_MS); // para simular un servicio lento
    const quantity = Number(query.get('quantity') ?? 1);
    if (!Number.isInteger(quantity) || quantity <= 0) {
      throw httpError(400, 'quantity debe ser un entero positivo');
    }
    const result = checkAvailability(params.productId, quantity);
    if (!result) throw httpError(404, `Producto ${params.productId} no existe`);
    console.log(`[REST] availability ${result.productId} x${quantity} -> ${result.available ? 'DISPONIBLE' : 'SIN STOCK'}`);
    sendJson(res, 200, result);
  });

  const server = http.createServer(router.handle);
  server.keepAliveTimeout = 65_000;
  server.listen(port, () => console.log(`[REST] InventoryService escuchando en http://0.0.0.0:${port}`));
  return server;
}

module.exports = { startRestServer };
