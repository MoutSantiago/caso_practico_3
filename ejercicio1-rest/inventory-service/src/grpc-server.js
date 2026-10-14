'use strict';

// Servidor gRPC equivalente al endpoint REST /availability (para comparar ambos estilos).
const path = require('node:path');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { checkAvailability } = require('./data');

const PROTO_PATH = process.env.PROTO_PATH || path.join(__dirname, '..', '..', 'proto', 'inventory.proto');
const LATENCY_MS = Number(process.env.SIMULATED_LATENCY_MS || 0);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function startGrpcServer(port) {
  const definition = protoLoader.loadSync(PROTO_PATH, { keepCase: false, longs: String, defaults: true });
  const { inventory } = grpc.loadPackageDefinition(definition);

  const server = new grpc.Server();
  server.addService(inventory.InventoryService.service, {
    CheckAvailability: async (call, callback) => {
      if (LATENCY_MS) await sleep(LATENCY_MS);
      const { productId, quantity } = call.request;
      if (!Number.isInteger(quantity) || quantity <= 0) {
        return callback({ code: grpc.status.INVALID_ARGUMENT, details: 'quantity debe ser un entero positivo' });
      }
      const result = checkAvailability(productId, quantity);
      if (!result) {
        return callback({ code: grpc.status.NOT_FOUND, details: `Producto ${productId} no existe` });
      }
      console.log(`[gRPC] CheckAvailability ${productId} x${quantity} -> ${result.available ? 'DISPONIBLE' : 'SIN STOCK'}`);
      return callback(null, result);
    },
  });

  server.bindAsync(`0.0.0.0:${port}`, grpc.ServerCredentials.createInsecure(), (err) => {
    if (err) {
      console.error('[gRPC] No se pudo iniciar el servidor:', err);
      process.exit(1);
    }
    console.log(`[gRPC] InventoryService escuchando en 0.0.0.0:${port}`);
  });
  return server;
}

module.exports = { startGrpcServer };
