'use strict';

const amqp = require('amqplib');
const { QUEUE, assertTopology } = require('./topology');
const { sendOrderCreatedEmail } = require('./mailer');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://admin:admin@localhost:5672';
const PREFETCH = Number(process.env.PREFETCH || 5);
// Para la demo: simula un envío lento y permite ver cómo se acumulan los mensajes.
const PROCESSING_DELAY_MS = Number(process.env.PROCESSING_DELAY_MS || 0);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
// Idempotencia: RabbitMQ garantiza entrega "al menos una vez", así que un mensaje puede
// llegar repetido (por ejemplo, si el servicio se cae después de enviar el mail y antes del ack).
const processed = new Set();

async function connectWithRetry() {
  for (let attempt = 1; ; attempt++) {
    try {
      return await amqp.connect(RABBITMQ_URL);
    } catch (err) {
      const wait = Math.min(1000 * attempt, 10_000);
      console.warn(`[amqp] No se pudo conectar (intento ${attempt}): ${err.message}. Reintento en ${wait} ms`);
      await sleep(wait);
    }
  }
}

async function handleMessage(channel, msg) {
  let event;
  try {
    event = JSON.parse(msg.content.toString());
  } catch {
    console.error('[notification] Mensaje con formato inválido -> DLQ');
    return channel.nack(msg, false, false);
  }

  if (processed.has(event.eventId)) {
    console.log(`[notification] Evento ${event.eventId} duplicado, se ignora`);
    return channel.ack(msg);
  }

  const delaySec = ((Date.now() - Date.parse(event.occurredAt)) / 1000).toFixed(1);
  console.log(`[notification] Recibido ${event.eventType} del pedido ${event.orderId} (publicado hace ${delaySec} s)`);

  try {
    if (PROCESSING_DELAY_MS) await sleep(PROCESSING_DELAY_MS);
    await sendOrderCreatedEmail(event);
    processed.add(event.eventId);
    channel.ack(msg); // recién ahora RabbitMQ borra el mensaje de la cola
    console.log(`[notification] Email enviado a ${event.data.customerEmail}`);
  } catch (err) {
    // Primer fallo: se devuelve a la cola para reintentar. Si ya era un reintento: a la DLQ.
    const requeue = !msg.fields.redelivered;
    console.error(`[notification] Falló el envío (${err.message}) -> ${requeue ? 'reencolado' : 'DLQ'}`);
    if (requeue) await sleep(2000);
    channel.nack(msg, false, requeue);
  }
}

async function main() {
  const connection = await connectWithRetry();
  connection.on('error', (err) => console.error('[amqp] Error de conexión:', err.message));
  connection.on('close', () => {
    // Docker (restart: unless-stopped) vuelve a levantar el proceso y se reconecta.
    console.error('[amqp] Conexión cerrada. Saliendo para reiniciar.');
    process.exit(1);
  });

  const channel = await connection.createChannel();
  await assertTopology(channel);
  await channel.prefetch(PREFETCH); // máximo de mensajes sin ack en vuelo
  await channel.consume(QUEUE, (msg) => msg && handleMessage(channel, msg), { noAck: false });
  console.log(`NotificationService escuchando la cola "${QUEUE}"`);

  const shutdown = async () => {
    console.log('Cerrando NotificationService...');
    connection.removeAllListeners('close');
    await connection.close().catch(() => {});
    process.exit(0);
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

main().catch((err) => {
  console.error('Error fatal:', err);
  process.exit(1);
});
