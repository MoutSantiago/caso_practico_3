# El contexto de build es la carpeta ejercicio1-rest (para poder copiar /proto).
FROM node:20-alpine
WORKDIR /app/orders-service
COPY orders-service/package.json ./
RUN npm install --omit=dev --no-audit --no-fund
COPY proto /app/proto
COPY orders-service/src ./src
ENV NODE_ENV=production
EXPOSE 3000
CMD ["node", "src/index.js"]
