'use strict';

const amqp = require('amqplib');
const { EXCHANGE, assertTopology } = require('./topology');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Publica eventos en RabbitMQ con "publisher confirms" (el broker confirma que guardó el mensaje).
// Si RabbitMQ no está disponible, los eventos quedan en una cola local (outbox simplificado
// en memoria) y se reenvían en orden cuando vuelve la conexión.
class EventPublisher {
  constructor(url) {
    this.url = url;
    this.channel = null;
    this.pending = [];
    this.connecting = false;
    this.flushing = false;
    this.ackTimeoutMs = Number(process.env.PUBLISH_TIMEOUT_MS || 3000);
  }

  start() {
    this.connect();
    setInterval(() => this.flush(), 3000).unref();
  }

  get connected() {
    return this.channel !== null;
  }

  async connect() {
    if (this.connecting) return;
    this.connecting = true;
    for (let attempt = 1; ; attempt++) {
      try {
        const connection = await amqp.connect(this.url);
        connection.on('error', (err) => console.error('[amqp] Error de conexión:', err.message));
        connection.on('close', () => {
          console.warn('[amqp] Conexión cerrada. Reintentando...');
          this.channel = null;
          setTimeout(() => this.connect(), 2000);
        });
        const channel = await connection.createConfirmChannel();
        channel.on('error', (err) => console.error('[amqp] Error de canal:', err.message));
        channel.on('close', () => {
          this.channel = null;
          connection.close().catch(() => {}); // fuerza la reconexión completa
        });
        await assertTopology(channel);
        this.channel = channel;
        this.connecting = false;
        console.log('[amqp] Conectado a RabbitMQ');
        this.flush();
        return;
      } catch (err) {
        const wait = Math.min(1000 * attempt, 10_000);
        console.warn(`[amqp] No se pudo conectar (intento ${attempt}): ${err.message}. Reintento en ${wait} ms`);
        await sleep(wait);
      }
    }
  }

  // Devuelve true si el broker confirmó el mensaje, false si quedó pendiente.
  // Los eventos se encolan siempre, para respetar el orden aunque haya pendientes.
  publish(routingKey, event) {
    return new Promise((notify) => {
      this.pending.push({ routingKey, event, notify });
      if (!this.channel) return notify(false);
      // No dejamos colgada la respuesta HTTP: si el broker tarda, el evento sigue pendiente.
      setTimeout(() => notify(false), this.ackTimeoutMs).unref();
      this.flush();
    });
  }

  async flush() {
    if (this.flushing || !this.channel) return;
    this.flushing = true;
    try {
      while (this.pending.length && this.channel) {
        const entry = this.pending[0];
        await this.send(entry.routingKey, entry.event);
        this.pending.shift();
        entry.notify(true);
        console.log(`[amqp] Publicado ${entry.event.eventType} ${entry.event.orderId} (routing key "${entry.routingKey}")`);
      }
    } catch (err) {
      console.warn(`[amqp] No se pudo publicar, ${this.pending.length} evento(s) pendiente(s): ${err.message}`);
      this.pending.forEach((entry) => entry.notify(false));
    } finally {
      this.flushing = false;
    }
  }

  send(routingKey, event) {
    return new Promise((resolve, reject) => {
      this.channel.publish(
        EXCHANGE,
        routingKey,
        Buffer.from(JSON.stringify(event)),
        {
          persistent: true, // deliveryMode 2: el mensaje se guarda en disco
          contentType: 'application/json',
          messageId: event.eventId,
          type: event.eventType,
          timestamp: Math.floor(Date.now() / 1000),
        },
        (err) => (err ? reject(err) : resolve()),
      );
    });
  }
}

module.exports = { EventPublisher };
