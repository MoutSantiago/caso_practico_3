'use strict';

// Micro-benchmark REST vs gRPC para la misma consulta de disponibilidad.
// Uso (con el compose levantado):  docker compose exec orders-service node src/benchmark.js
const { createRestInventoryClient, createGrpcInventoryClient } = require('./inventory-client');

const N = Number(process.env.BENCH_REQUESTS || 2000);
const CONCURRENCY = Number(process.env.BENCH_CONCURRENCY || 50);
const PRODUCT = process.env.BENCH_PRODUCT || 'P001';

const rest = createRestInventoryClient({
  baseUrl: process.env.INVENTORY_REST_URL || 'http://localhost:3001',
  timeoutMs: 5000,
});
const grpcClient = createGrpcInventoryClient({
  address: process.env.INVENTORY_GRPC_ADDR || 'localhost:50051',
  timeoutMs: 5000,
});

const percentile = (sorted, p) => sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];

async function run(name, client) {
  for (let i = 0; i < 50; i++) await client.checkAvailability(PRODUCT, 1); // warm-up

  // 1) Secuencial: latencia de una llamada aislada
  const latencies = [];
  for (let i = 0; i < N; i++) {
    const t = performance.now();
    await client.checkAvailability(PRODUCT, 1);
    latencies.push(performance.now() - t);
  }
  latencies.sort((a, b) => a - b);

  // 2) Concurrente: throughput con varias llamadas en vuelo
  let next = 0;
  const t0 = performance.now();
  await Promise.all(
    Array.from({ length: CONCURRENCY }, async () => {
      while (next++ < N) await client.checkAvailability(PRODUCT, 1);
    }),
  );
  const seconds = (performance.now() - t0) / 1000;

  return {
    protocolo: name,
    'p50 (ms)': +percentile(latencies, 50).toFixed(3),
    'p95 (ms)': +percentile(latencies, 95).toFixed(3),
    'p99 (ms)': +percentile(latencies, 99).toFixed(3),
    [`req/s (conc=${CONCURRENCY})`]: Math.round(N / seconds),
  };
}

(async () => {
  console.log(`Benchmark: ${N} llamadas por protocolo, producto ${PRODUCT}\n`);
  const results = [await run('REST/JSON', rest), await run('gRPC/Protobuf', grpcClient)];
  console.table(results);

  // Tamaño del cuerpo de la respuesta (sin contar headers HTTP).
  const sample = await rest.checkAvailability(PRODUCT, 1);
  const jsonBytes = Buffer.byteLength(JSON.stringify(sample));
  const method = grpcClient.definition['inventory.InventoryService'].CheckAvailability;
  const protoBytes = method.responseSerialize(sample).length;
  console.log(`Tamaño del payload de respuesta: JSON = ${jsonBytes} bytes | Protobuf = ${protoBytes} bytes`);
  process.exit(0);
})().catch((err) => {
  console.error('Error en el benchmark:', err.message);
  process.exit(1);
});
