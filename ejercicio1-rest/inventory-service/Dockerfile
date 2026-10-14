# El contexto de build es la carpeta ejercicio1-rest (para poder copiar /proto).
FROM node:20-alpine
WORKDIR /app/inventory-service
COPY inventory-service/package.json ./
RUN npm install --omit=dev --no-audit --no-fund
COPY proto /app/proto
COPY inventory-service/src ./src
ENV NODE_ENV=production
EXPOSE 3001 50051
CMD ["node", "src/index.js"]
