var port = Api.EnvInt("PORT", 3000);
var rabbitUrl = Environment.GetEnvironmentVariable("RABBITMQ_URL") ?? "amqp://admin:admin@localhost:5672";

var inventory = InventoryClients.Create();
var publisher = new EventPublisher(rabbitUrl);
var orders = new Dictionary<string, Order>();
var ordersLock = new object();

var builder = WebApplication.CreateBuilder(args);
builder.Logging.SetMinimumLevel(LogLevel.Warning);
builder.WebHost.ConfigureKestrel(o => o.ListenAnyIP(port));
var app = builder.Build();
app.UseJsonErrors();

app.MapGet("/health", () => Api.Reply(200, new
{
    status = "ok",
    service = "orders-service",
    rabbitmq = publisher.Connected ? "conectado" : "desconectado",
    pendingEvents = publisher.PendingCount,
}));

app.MapGet("/orders", () =>
{
    lock (ordersLock) return Api.Reply(200, orders.Values.ToList());
});

app.MapGet("/orders/{id}", (string id) =>
{
    Order? order;
    lock (ordersLock) orders.TryGetValue(id, out order);
    return order is null ? throw new HttpError(404, "Pedido no encontrado") : Api.Reply(200, order);
});

app.MapPost("/orders", async (HttpRequest req) =>
{
    var input = OrderValidation.Validate(await Api.ReadJsonAsync(req));

    // 1) Verificación de stock SÍNCRONA (igual que en el ejercicio 1).
    Availability availability;
    try
    {
        availability = await InventoryClients.WithRetry(() => inventory.CheckAvailabilityAsync(input.ProductId, input.Quantity));
    }
    catch (InventoryException e) when (e.Code == InventoryErrorCode.NotFound)
    {
        throw new HttpError(404, e.Message);
    }
    catch (InventoryException e)
    {
        return Api.Reply(503, new { error = "InventoryService no disponible", detail = e.Message });
    }
    if (!availability.Available)
        throw new HttpError(409, $"Stock insuficiente de {input.ProductId} (disponible: {availability.Stock})");

    var order = new Order(
        Guid.NewGuid().ToString(),
        input.ProductId,
        input.Quantity,
        input.CustomerEmail,
        availability.Name,
        "CREADO",
        DateTime.UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'"));
    lock (ordersLock) orders[order.Id] = order;

    // 2) Notificación ASÍNCRONA: publicamos el evento y respondemos sin esperar el envío del mail.
    var ev = new OrderEvent(
        Guid.NewGuid().ToString(),
        "PedidoCreado",
        order.CreatedAt,
        order.Id,
        new OrderEventData(order.CustomerEmail, order.ProductId, order.ProductName, order.Quantity));
    var published = await publisher.PublishAsync(Topology.RoutingKey, ev);

    Console.WriteLine($"[orders] Pedido {order.Id} creado (evento {(published ? "publicado" : "pendiente de envío")})");
    return Api.Reply(201, order with { EventPublished = published });
});

publisher.Start();
app.Lifetime.ApplicationStarted.Register(() => Console.WriteLine($"OrdersService escuchando en http://0.0.0.0:{port}"));
app.Run();

record Order(
    string Id,
    string ProductId,
    int Quantity,
    string CustomerEmail,
    string ProductName,
    string Status,
    string CreatedAt,
    bool? EventPublished = null);
