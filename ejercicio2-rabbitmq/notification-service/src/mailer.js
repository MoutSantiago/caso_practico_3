'use strict';

const nodemailer = require('nodemailer');

// SMTP de pruebas: Mailpit captura los correos y los muestra en http://localhost:8025
const transporter = nodemailer.createTransport({
  host: process.env.SMTP_HOST || 'localhost',
  port: Number(process.env.SMTP_PORT || 1025),
  secure: false,
});

const FROM = process.env.MAIL_FROM || 'Tienda SD <no-reply@tienda-sd.local>';

async function sendOrderCreatedEmail(event) {
  const { orderId, data } = event;
  const shortId = orderId.slice(0, 8);
  await transporter.sendMail({
    from: FROM,
    to: data.customerEmail,
    subject: `Recibimos tu pedido #${shortId}`,
    text:
      `¡Hola! Recibimos tu pedido #${shortId}.\n\n` +
      `Producto: ${data.productName} (${data.productId})\nCantidad: ${data.quantity}\n\n` +
      `Te vamos a avisar cuando sea despachado.`,
    html:
      `<h2>¡Gracias por tu compra!</h2>` +
      `<p>Recibimos tu pedido <b>#${shortId}</b>.</p>` +
      `<ul><li>Producto: ${data.productName} (${data.productId})</li><li>Cantidad: ${data.quantity}</li></ul>` +
      `<p>Te vamos a avisar cuando sea despachado.</p>`,
  });
}

module.exports = { sendOrderCreatedEmail };
