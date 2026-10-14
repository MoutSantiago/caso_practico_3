'use strict';

const { Kafka, logLevel } = require('kafkajs');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Publica los eventos de estado del pedido en un topic de Kafka.
// - key = orderId: todos los eventos de un mismo pedido van a la MISMA partición, así que
//   los consumidores los leen en orden (creado -> confirmado -> enviado).
// - Productor idempotente + acks=all: sin duplicados por reintentos y sin pérdida si el broker confirma.
// - Si Kafka no está disponible, los eventos quedan pendientes en memoria y se reenvían en orden.
class EventPublisher {
  constructor({ brokers, topic, partitions = 3 }) {
    this.topic = topic;
    this.partitions = partitions;
    this.kafka = new Kafka({
      clientId: 'orders-service',
      brokers,
      logLevel: logLevel.WARN,
      retry: { initialRetryTime: 300, retries: 8 },
    });
    this.producer = this.kafka.producer({ idempotent: true, maxInFlightRequests: 1, allowAutoTopicCreation: false });
    this.ready = false;
    this.pending = [];
    this.flushing = false;
    this.ackTimeoutMs = Number(process.env.PUBLISH_TIMEOUT_MS || 3000);
  }

  async start() {
    for (let attempt = 1; ; attempt++) {
      try {
        await this.ensureTopic();
        await this.producer.connect();
        this.ready = true;
        console.log(`[kafka] Productor conectado. Topic "${this.topic}" (${this.partitions} particiones)`);
        setInterval(() => this.flush(), 3000).unref();
        this.flush();
        return;
      } catch (err) {
        const wait = Math.min(1000 * attempt, 10_000);
        console.warn(`[kafka] No se pudo conectar (intento ${attempt}): ${err.message}. Reintento en ${wait} ms`);
        await sleep(wait);
      }
    }
  }

  async ensureTopic() {
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      const created = await admin.createTopics({
        waitForLeaders: true,
        topics: [{ topic: this.topic, numPartitions: this.partitions, replicationFactor: 1 }],
      });
      if (created) console.log(`[kafka] Topic "${this.topic}" creado`);
    } finally {
      await admin.disconnect();
    }
  }

  // Devuelve true si Kafka confirmó la escritura, false si quedó pendiente.
  publish(event) {
    return new Promise((notify) => {
      this.pending.push({ event, notify });
      if (!this.ready) return notify(false);
      // No dejamos colgada la respuesta HTTP: si el broker tarda, el evento sigue pendiente.
      setTimeout(() => notify(false), this.ackTimeoutMs).unref();
      this.flush();
    });
  }

  async flush() {
    if (this.flushing || !this.ready) return;
    this.flushing = true;
    try {
      while (this.pending.length) {
        const entry = this.pending[0];
        const [meta] = await this.producer.send({
          topic: this.topic,
          acks: -1, // "all": espera a todas las réplicas sincronizadas
          messages: [
            {
              key: entry.event.orderId,
              value: JSON.stringify(entry.event),
              headers: { eventType: entry.event.eventType },
            },
          ],
        });
        this.pending.shift();
        entry.notify(true);
        console.log(
          `[kafka] ${entry.event.eventType} pedido ${entry.event.orderId.slice(0, 8)} -> partición ${meta.partition}, offset ${meta.baseOffset}`,
        );
      }
    } catch (err) {
      console.warn(`[kafka] No se pudo publicar, ${this.pending.length} evento(s) pendiente(s): ${err.message}`);
      this.pending.forEach((entry) => entry.notify(false));
    } finally {
      this.flushing = false;
    }
  }
}

module.exports = { EventPublisher };
