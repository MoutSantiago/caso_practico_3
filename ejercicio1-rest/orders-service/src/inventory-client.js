'use strict';

// Clientes para consultar InventoryService. Ambos exponen la misma interfaz:
//   checkAvailability(productId, quantity) -> { productId, name, requested, stock, available }
// y lanzan InventoryError con code 'NOT_FOUND' o 'UNAVAILABLE'.
const path = require('node:path');

class InventoryError extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
  }
}

// ---------- REST (HTTP/1.1 + JSON) ----------
function createRestInventoryClient({ baseUrl, timeoutMs }) {
  return {
    protocol: 'rest',
    async checkAvailability(productId, quantity) {
      const url = `${baseUrl}/inventory/${encodeURIComponent(productId)}/availability?quantity=${quantity}`;
      let res;
      try {
        res = await fetch(url, { signal: AbortSignal.timeout(timeoutMs) });
      } catch (err) {
        const reason = err.name === 'TimeoutError' ? `timeout de ${timeoutMs} ms` : err.cause?.code || err.message;
        throw new InventoryError('UNAVAILABLE', `InventoryService no respondió (${reason})`);
      }
      const body = await res.json().catch(() => ({}));
      if (res.status === 404) throw new InventoryError('NOT_FOUND', body.error || `Producto ${productId} no existe`);
      if (!res.ok) throw new InventoryError('UNAVAILABLE', `InventoryService respondió HTTP ${res.status}`);
      return body;
    },
  };
}

// ---------- gRPC (HTTP/2 + Protocol Buffers) ----------
function createGrpcInventoryClient({ address, timeoutMs }) {
  // require "perezoso": el modo REST funciona aunque no estén instaladas las libs de gRPC.
  const grpc = require('@grpc/grpc-js');
  const protoLoader = require('@grpc/proto-loader');
  const protoPath = process.env.PROTO_PATH || path.join(__dirname, '..', '..', 'proto', 'inventory.proto');
  const definition = protoLoader.loadSync(protoPath, { keepCase: false, longs: String, defaults: true });
  const { inventory } = grpc.loadPackageDefinition(definition);
  const client = new inventory.InventoryService(address, grpc.credentials.createInsecure());

  return {
    protocol: 'grpc',
    definition,
    checkAvailability(productId, quantity) {
      return new Promise((resolve, reject) => {
        const deadline = Date.now() + timeoutMs;
        client.CheckAvailability({ productId, quantity }, { deadline }, (err, reply) => {
          if (!err) return resolve(reply);
          if (err.code === grpc.status.NOT_FOUND) return reject(new InventoryError('NOT_FOUND', err.details));
          const name = Object.keys(grpc.status).find((k) => grpc.status[k] === err.code);
          return reject(new InventoryError('UNAVAILABLE', `InventoryService (gRPC) ${name}: ${err.details}`));
        });
      });
    },
  };
}

function createInventoryClient(env = process.env) {
  const timeoutMs = Number(env.INVENTORY_TIMEOUT_MS || 2000);
  if ((env.INVENTORY_PROTOCOL || 'rest').toLowerCase() === 'grpc') {
    return createGrpcInventoryClient({ address: env.INVENTORY_GRPC_ADDR || 'localhost:50051', timeoutMs });
  }
  return createRestInventoryClient({ baseUrl: env.INVENTORY_REST_URL || 'http://localhost:3001', timeoutMs });
}

// Reintenta sólo errores transitorios (servicio caído / timeout), nunca un 404.
async function withRetry(fn, { retries = 2, delayMs = 200 } = {}) {
  for (let attempt = 0; ; attempt++) {
    try {
      return await fn();
    } catch (err) {
      if (err.code !== 'UNAVAILABLE' || attempt >= retries) throw err;
      const wait = delayMs * 2 ** attempt;
      console.warn(`[inventory] ${err.message}. Reintento ${attempt + 1}/${retries} en ${wait} ms`);
      await new Promise((r) => setTimeout(r, wait));
    }
  }
}

module.exports = {
  InventoryError,
  createInventoryClient,
  createRestInventoryClient,
  createGrpcInventoryClient,
  withRetry,
};
