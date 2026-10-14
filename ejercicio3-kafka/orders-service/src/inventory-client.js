'use strict';

// Cliente REST de InventoryService (el mismo del ejercicio 1, sin la variante gRPC).
//   checkAvailability(productId, quantity) -> { productId, name, requested, stock, available }
// Lanza InventoryError con code 'NOT_FOUND' o 'UNAVAILABLE'.

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

function createInventoryClient(env = process.env) {
  return createRestInventoryClient({
    baseUrl: env.INVENTORY_REST_URL || 'http://localhost:3001',
    timeoutMs: Number(env.INVENTORY_TIMEOUT_MS || 2000),
  });
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
  withRetry,
};
