'use strict';

const nodemailer = require('nodemailer');

// SMTP de pruebas: Mailpit captura los correos y los muestra en http://localhost:8025
const transporter = nodemailer.createTransport({
  host: process.env.SMTP_HOST || 'localhost',
  port: Number(process.env.SMTP_PORT || 1025),
  secure: false,
});

const FROM = process.env.MAIL_FROM || 'Tienda SD <no-reply@tienda-sd.local>';

// Plantilla de correo según el tipo de evento.
const TEMPLATES = {
  OrderCreated: (id, d) => ({
    subject: `Recibimos tu pedido #${id}`,
    body: `Recibimos tu pedido de ${d.quantity} x ${d.productName}. Estamos verificando el stock.`,
  }),
  OrderConfirmed: (id, d) => ({
    subject: `Tu pedido #${id} fue confirmado`,
    body: `¡Buenas noticias! Confirmamos tu pedido de ${d.quantity} x ${d.productName}.`,
  }),
  OrderRejected: (id, d) => ({
    subject: `No pudimos confirmar tu pedido #${id}`,
    body: `Lo sentimos, no pudimos confirmar tu pedido de ${d.productName}. Motivo: ${d.reason}.`,
  }),
  OrderShipped: (id, d) => ({
    subject: `Tu pedido #${id} está en camino`,
    body: `Despachamos tu pedido de ${d.quantity} x ${d.productName}. Código de seguimiento: ${d.trackingCode}.`,
  }),
};

async function sendOrderEmail(event) {
  const template = TEMPLATES[event.eventType];
  if (!template) return false; // evento que no requiere notificación
  const { subject, body } = template(event.orderId.slice(0, 8), event.data);
  await transporter.sendMail({
    from: FROM,
    to: event.data.customerEmail,
    subject,
    text: `¡Hola!\n\n${body}\n\nEstado actual: ${event.status}`,
    html: `<h2>${subject}</h2><p>${body}</p><p><small>Estado actual: <b>${event.status}</b></small></p>`,
  });
  return true;
}

module.exports = { sendOrderEmail };
