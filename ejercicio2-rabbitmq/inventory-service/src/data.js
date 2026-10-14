'use strict';

// "Base de datos" en memoria del inventario.
const products = new Map([
  ['P001', { id: 'P001', name: 'Teclado mecánico', stock: 10 }],
  ['P002', { id: 'P002', name: 'Mouse inalámbrico', stock: 0 }],
  ['P003', { id: 'P003', name: 'Monitor 27"', stock: 3 }],
]);

function checkAvailability(productId, quantity) {
  const product = products.get(productId);
  if (!product) return null;
  return {
    productId: product.id,
    name: product.name,
    requested: quantity,
    stock: product.stock,
    available: product.stock >= quantity,
  };
}

module.exports = { products, checkAvailability };
