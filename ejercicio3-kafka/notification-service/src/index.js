'use strict';

const { Kafka, logLevel } = require('kafkajs');
const { sendOrderEmail } = require('./mailer');

const TOPIC = process.env.ORDER_EVENTS_TOPIC || 'order-events';
const GROUP_ID = process.env.KAFKA_GROUP_ID || 'notification-service';

const kafka = new Kafka({
  clientId: 'notification-service',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  logLevel: logLevel.WARN,
  retry: { initialRetryTime: 500, retries: 10 },
});

// Consumer group propio: recibe TODOS los eventos del topic, independientemente de
// lo que lea AnalyticsService (que está en otro grupo).
const consumer = kafka.consumer({ groupId: GROUP_ID });

// Idempotencia: Kafka entrega "al menos una vez". Si el servicio se cae después de enviar
// el mail y antes de confirmar el offset, el evento se vuelve a leer.
const processed = new Set();

async function main() {
  await consumer.connect();
  // fromBeginning sólo aplica la primera vez que el grupo se conecta (no tiene offsets guardados).
  // Después, Kafka recuerda el último offset confirmado del grupo y sigue desde ahí.
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

  await consumer.run({
    // El offset se confirma automáticamente sólo si eachMessage termina sin error.
    // Si el SMTP falla, kafkajs reintenta el mismo mensaje (no se pierde).
    eachMessage: async ({ partition, message }) => {
      const event = JSON.parse(message.value.toString());
      if (processed.has(event.eventId)) return;

      const delaySec = ((Date.now() - Date.parse(event.occurredAt)) / 1000).toFixed(1);
      const sent = await sendOrderEmail(event);
      processed.add(event.eventId);
      console.log(
        `[notification] p${partition}@${message.offset} ${event.eventType} pedido ${event.orderId.slice(0, 8)}` +
          (sent ? ` -> mail a ${event.data.customerEmail}` : ' (sin mail)') +
          ` (publicado hace ${delaySec} s)`,
      );
    },
  });
  console.log(`NotificationService consumiendo "${TOPIC}" como grupo "${GROUP_ID}"`);
}

consumer.on(consumer.events.CRASH, ({ payload }) => {
  console.error('[kafka] El consumidor falló:', payload.error.message);
  if (!payload.restart) process.exit(1); // Docker lo reinicia
});

const shutdown = async () => {
  console.log('Cerrando NotificationService...');
  await consumer.disconnect().catch(() => {});
  process.exit(0);
};
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

main().catch((err) => {
  console.error('Error fatal:', err.message);
  process.exit(1); // Docker (restart: unless-stopped) lo vuelve a levantar
});
