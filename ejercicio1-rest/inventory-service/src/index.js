'use strict';

const { startRestServer } = require('./rest-server');

startRestServer(Number(process.env.PORT || 3001));

// gRPC es opcional: sólo se usa para comparar con REST.
if (process.env.ENABLE_GRPC !== 'false') {
  const { startGrpcServer } = require('./grpc-server');
  startGrpcServer(Number(process.env.GRPC_PORT || 50051));
}
