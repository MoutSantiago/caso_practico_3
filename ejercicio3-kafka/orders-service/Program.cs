using System.Text.Json;
using System.Text.Json.Nodes;

var port = Api.EnvInt("PORT", 3000);

var inventory = InventoryClients.Create();
var publisher = new EventPublisher(
    Environment.GetEnvironmentVariable("KAFKA_BROKERS") ?? "localhost:9092",
    Environment.GetEnvironmentVariable("ORDER_EVENTS_TOPIC") ?? "order-events",
    Api.EnvInt("ORDER_EVENTS_PARTITIONS", 3));
var orders = new Dictionary<string, Order>();
var ordersLock = new object();

// Máquina de estados del pedido:
//   CREADO -> CONFIRMADO -> ENVIADO
//   CREADO -> RECHAZADO (sin stock)
var eventByStatus = new Dictionary<string, string>
{
    ["CREADO"] = "OrderCreated",
    ["CONFIRMADO"] = "OrderConfirmed",
    ["RECHAZADO"] = "OrderRejected",
    ["ENVIADO"] = "OrderShipped",
};

var builder = WebApplication.CreateBuilder(args);
builder.Logging.SetMinimumLevel(LogLevel.Warning);
builder.WebHost.ConfigureKestrel(o => o.ListenAnyIP(port));
var app = builder.Build();
app.UseJsonErrors();

// Cambia el estado y publica el evento correspondiente en Kafka.
async Task Transition(Order order, string newStatus, string? reason = null, string? trackingCode = null)
{
    var previousStatus = order.Status;
    var at = Now();
    order.Status = newStatus;
    order.UpdatedAt = at;
    if (reason is not null) order.Reason = reason;
    if (trackingCode is not null) order.TrackingCode = trackingCode;
    order.History.Add(new HistoryEntry(newStatus, at));

    var ev = new OrderEvent(
        Guid.NewGuid().ToString(),
        eventByStatus[newStatus],
        order.Id,
        newStatus,
        previousStatus,
        at,
        new OrderEventData(order.CustomerEmail, order.ProductId, order.ProductName, order.Quantity, reason, trackingCode));
    order.LastEventPublished = await publisher.PublishAsync(ev);
    Console.WriteLine($"[orders] Pedido {order.Id[..8]}: {previousStatus ?? "-"} -> {newStatus}");
}

// Consulta stock (REST síncrono, como en el ejercicio 1) y confirma o rechaza.
async Task ConfirmWithInventory(Order order)
{
    var availability = await InventoryClients.WithRetry(() => inventory.CheckAvailabilityAsync(order.ProductId, order.Quantity));
    if (availability.Available) await Transition(order, "CONFIRMADO");
    else await Transition(order, "RECHAZADO", reason: $"Stock insuficiente (disponible: {availability.Stock})");
}

Order GetOrder(string id)
{
    lock (ordersLock)
        return orders.TryGetValue(id, out var order) ? order : throw new HttpError(404, "Pedido no encontrado");
}

app.MapGet("/health", () => Api.Reply(200, new
{
    status = "ok",
    service = "orders-service",
    kafka = publisher.Ready ? "conectado" : "desconectado",
    pendingEvents = publisher.PendingCount,
}));

app.MapGet("/orders", () =>
{
    lock (ordersLock) return Api.Reply(200, orders.Values.ToList());
});
app.MapGet("/orders/{id}", (string id) => Api.Reply(200, GetOrder(id)));

// Crea el pedido (evento OrderCreated) e intenta confirmarlo contra el inventario
// (evento OrderConfirmed u OrderRejected).
app.MapPost("/orders", async (HttpRequest req) =>
{
    var input = OrderValidation.Validate(await Api.ReadJsonAsync(req));

    // Validamos que el producto exista antes de crear nada.
    Availability? product;
    try
    {
        product = await InventoryClients.WithRetry(() => inventory.CheckAvailabilityAsync(input.ProductId, input.Quantity));
    }
    catch (InventoryException e) when (e.Code == InventoryErrorCode.NotFound)
    {
        throw new HttpError(404, e.Message);
    }
    catch (InventoryException)
    {
        product = null; // Inventory caído: el pedido queda CREADO y se confirma después
    }

    var order = new Order
    {
        Id = Guid.NewGuid().ToString(),
        ProductId = input.ProductId,
        Quantity = input.Quantity,
        CustomerEmail = input.CustomerEmail,
        ProductName = product?.Name ?? input.ProductId,
    };
    lock (ordersLock) orders[order.Id] = order;
    await Transition(order, "CREADO");

    if (product is null)
    {
        var node = JsonSerializer.SerializeToNode(order, Api.JsonOptions)!.AsObject();
        node["message"] = "InventoryService no disponible: el pedido quedó CREADO. Confirmalo luego con POST /orders/:id/confirm";
        return Api.Reply(202, node);
    }
    if (product.Available) await Transition(order, "CONFIRMADO");
    else await Transition(order, "RECHAZADO", reason: $"Stock insuficiente (disponible: {product.Stock})");
    return Api.Reply(201, order);
});

// Reintenta la confirmación de un pedido que quedó CREADO.
app.MapPost("/orders/{id}/confirm", async (string id) =>
{
    var order = GetOrder(id);
    if (order.Status != "CREADO") throw new HttpError(409, $"No se puede confirmar un pedido en estado {order.Status}");
    try
    {
        await ConfirmWithInventory(order);
    }
    catch (InventoryException e)
    {
        return Api.Reply(503, new { error = "InventoryService no disponible", detail = e.Message });
    }
    return Api.Reply(200, order);
});

// Despacha el pedido (evento OrderShipped).
app.MapPost("/orders/{id}/ship", async (string id) =>
{
    var order = GetOrder(id);
    if (order.Status != "CONFIRMADO")
        throw new HttpError(409, $"Sólo se envían pedidos CONFIRMADOS (estado actual: {order.Status})");
    await Transition(order, "ENVIADO", trackingCode: $"AR{Random.Shared.Next(0, 1_000_000_000)}");
    return Api.Reply(200, order);
});

_ = publisher.StartAsync();
app.Lifetime.ApplicationStarted.Register(() => Console.WriteLine($"OrdersService escuchando en http://0.0.0.0:{port}"));
app.Run();

static string Now() => DateTime.UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'");

record HistoryEntry(string Status, string At);

class Order
{
    public required string Id { get; init; }
    public required string ProductId { get; init; }
    public required int Quantity { get; init; }
    public required string CustomerEmail { get; init; }
    public required string ProductName { get; init; }
    public string? Status { get; set; }
    public string? UpdatedAt { get; set; }
    public string? Reason { get; set; }
    public string? TrackingCode { get; set; }
    public List<HistoryEntry> History { get; } = [];
    public bool? LastEventPublished { get; set; }
}
