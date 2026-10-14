// Utilidades HTTP mínimas sobre node:http (sin Express, para no ocultar el protocolo).
'use strict';

function sendJson(res, status, body) {
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': Buffer.byteLength(payload),
  });
  res.end(payload);
}

function httpError(status, message) {
  return Object.assign(new Error(message), { status });
}

function readJson(req, limit = 1_000_000) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.setEncoding('utf8');
    req.on('data', (chunk) => {
      data += chunk;
      if (data.length > limit) {
        reject(httpError(413, 'Body demasiado grande'));
        req.destroy();
      }
    });
    req.on('end', () => {
      if (!data) return resolve({});
      try {
        resolve(JSON.parse(data));
      } catch {
        reject(httpError(400, 'El body no es JSON válido'));
      }
    });
    req.on('error', reject);
  });
}

// Router muy simple: router.get('/orders/:id', handler)
function createRouter() {
  const routes = [];
  const add = (method, pattern, handler) => {
    const keys = [];
    const source = pattern.replace(/:(\w+)/g, (_, key) => {
      keys.push(key);
      return '([^/]+)';
    });
    routes.push({ method, regex: new RegExp(`^${source}/?$`), keys, handler });
  };

  const handle = async (req, res) => {
    const url = new URL(req.url, 'http://localhost');
    for (const route of routes) {
      if (route.method !== req.method) continue;
      const match = url.pathname.match(route.regex);
      if (!match) continue;
      const params = Object.fromEntries(
        route.keys.map((key, i) => [key, decodeURIComponent(match[i + 1])]),
      );
      try {
        return await route.handler({ req, res, params, query: url.searchParams });
      } catch (err) {
        if (!err.status) console.error(err);
        return sendJson(res, err.status || 500, { error: err.status ? err.message : 'Error interno' });
      }
    }
    return sendJson(res, 404, { error: `Ruta no encontrada: ${req.method} ${url.pathname}` });
  };

  return {
    get: (pattern, handler) => add('GET', pattern, handler),
    post: (pattern, handler) => add('POST', pattern, handler),
    handle,
  };
}

module.exports = { sendJson, readJson, httpError, createRouter };
