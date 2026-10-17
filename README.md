# Caso práctico 3: Comunicación en Sistemas Distribuidos

Prototipos de un sistema de pedidos con tres estilos de comunicación entre microservicios:

| # | Carpeta | Estilo | Tecnologías | Pregunta |
|---|---|---|---|---|
| 1 | [`ejercicio1-rest`](ejercicio1-rest/) | Síncrono (request/response) | REST (+ gRPC para comparar) | [gRPC vs REST](ejercicio1-rest/README.md#comparación-grpc-vs-rest-en-este-escenario) |
| 2 | [`ejercicio2-rabbitmq`](ejercicio2-rabbitmq/) | Asíncrono, cola de mensajes | RabbitMQ, Docker | [¿Qué pasa si NotificationService está caído?](ejercicio2-rabbitmq/README.md#pregunta-qué-ocurre-si-notificationservice-está-caído-temporalmente) |
| 3 | [`ejercicio3-kafka`](ejercicio3-kafka/) | Asíncrono, streaming de eventos | Kafka (KRaft), Docker Compose | [Kafka vs RabbitMQ](ejercicio3-kafka/README.md#pregunta-qué-diferencias-notarías-entre-usar-kafka-y-rabbitmq-en-este-escenario) |

Cada ejercicio es independiente (tiene su propio `docker-compose.yml`) y extiende al anterior:

```
Ej. 1   Orders ──REST──► Inventory
Ej. 2   Orders ──REST──► Inventory
          └──"PedidoCreado"──► RabbitMQ ──► Notification ──► mail
Ej. 3   Orders ──REST──► Inventory
          └──creado/confirmado/rechazado/enviado──► Kafka ──┬─► Notification ──► mail
                                                            └─► Analytics ──► métricas
```

## Requisitos

- **Docker Desktop** (con Docker Compose v2).
- Opcional: .NET 8 SDK si se quiere correr algún servicio fuera de Docker (`dotnet run`).

Los servicios están escritos en **C# / .NET 8** con ASP.NET Core minimal APIs (sin MVC, para que se vea el protocolo). Las
librerías son las clientes oficiales o de facto de cada tecnología: `Grpc.AspNetCore` / `Grpc.Net.Client`, `RabbitMQ.Client`, `Confluent.Kafka` y
`System.Net.Mail`. Los correos se capturan con **Mailpit** (http://localhost:8025), así que no se envía nada real.

## Cómo correr un ejercicio

```bash
cd ejercicio2-rabbitmq        # o ejercicio1-rest / ejercicio3-kafka
docker compose up --build     # Ctrl+C para frenar
docker compose down           # borra contenedores (agregar -v para borrar también los volúmenes)
```

> Los tres usan el puerto 3000 (Orders) y el 2 y el 3 comparten Mailpit (8025): levantá **uno por vez**.

Productos de prueba en InventoryService: `P001` (stock 10), `P002` (stock 0) y `P003` (stock 3).

### Probar desde Windows

- **Git Bash:** los `curl` de los README funcionan tal cual.
- **VS Code:** cada ejercicio tiene un `requests.http` para la extensión *REST Client*.
- **PowerShell:**

  ```powershell
  $body = @{ productId = "P001"; quantity = 2; customerEmail = "ana@example.com" } | ConvertTo-Json
  Invoke-RestMethod -Method Post -Uri http://localhost:3000/orders -ContentType "application/json" -Body $body
  Invoke-RestMethod http://localhost:3000/orders
  ```

  (En Windows PowerShell 5, las respuestas 4xx/5xx aparecen como excepción. En PowerShell 7 se puede agregar
  `-SkipHttpErrorCheck`.)

## Resumen de las respuestas

1. **gRPC vs REST:** para una llamada interna, frecuente y en la ruta crítica como "¿hay stock?", gRPC ofrece un
   contrato tipado (`.proto`), payloads más chicos (31 B vs 89 B), HTTP/2 multiplexado y deadlines. REST es más
   simple, universal y fácil de depurar. **Ambos son síncronos:** si Inventory se cae, Orders no puede confirmar.
2. **NotificationService caído:** los pedidos se siguen creando y los mensajes **se acumulan** en la cola durable de
   RabbitMQ. Al volver, el servicio procesa el backlog y manda los correos con demora. No se pierde nada, gracias a
   la cola durable, los mensajes persistentes y el ack manual. Se pueden repetir, así que el consumidor tiene que
   ser idempotente.
3. **Kafka vs RabbitMQ:** RabbitMQ es una cola que **borra** el mensaje al consumirlo y enruta en el broker. Kafka es
   un **log** que conserva los eventos, permite que varios grupos lean de forma independiente, admite **replay** y
   garantiza **orden por partición** (key = orderId). Para eventos de estado con varios consumidores, Kafka encaja
   mejor. Para "mandar un mail", RabbitMQ es más simple.
