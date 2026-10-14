'use strict';

// Topología de RabbitMQ. Productor y consumidor la declaran igual (las declaraciones son
// idempotentes). Así la cola existe aunque NotificationService nunca haya arrancado y
// los mensajes no se pierden.
//
//   OrdersService --(order.created)--> [exchange orders.events (topic)]
//                                           └──> [cola notifications.order-created] --> NotificationService
//                                                     └─ rechazados ──> [orders.events.dlx] ──> [cola ...dlq]

const EXCHANGE = 'orders.events';
const ROUTING_KEY = 'order.created';
const QUEUE = 'notifications.order-created';
const DLX = 'orders.events.dlx';
const DLQ = 'notifications.order-created.dlq';

async function assertTopology(channel) {
  await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
  await channel.assertExchange(DLX, 'fanout', { durable: true });
  await channel.assertQueue(DLQ, { durable: true });
  await channel.bindQueue(DLQ, DLX, '');
  await channel.assertQueue(QUEUE, {
    durable: true, // la cola sobrevive a reinicios del broker
    arguments: { 'x-dead-letter-exchange': DLX },
  });
  await channel.bindQueue(QUEUE, EXCHANGE, ROUTING_KEY);
}

module.exports = { EXCHANGE, ROUTING_KEY, QUEUE, DLX, DLQ, assertTopology };
