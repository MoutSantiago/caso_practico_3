using System.Diagnostics;

// Micro-benchmark REST vs gRPC:  docker compose exec orders-service dotnet OrdersService.dll benchmark
if (args.Length > 0 && args[0] == "benchmark") return await Benchmark.RunAsync();

var port = Api.EnvInt("PORT", 3000);
var retries = Api.EnvInt("INVENTORY_RETRIES", 2);

var inventory = InventoryClients.Create();
var orders = new Dictionary<string, Order>();
var ordersLock = new object();

var builder = WebApplication.CreateBuilder(args);
builder.Logging.SetMinimumLevel(LogLevel.Warning);
builder.WebHost.ConfigureKestrel(o => o.ListenAnyIP(port));
var app = builder.Build();
app.UseJsonErrors();

app.MapGet("/health", () =>
    Api.Reply(200, new { status = "ok", service = "orders-service", inventoryProtocol = inventory.Protocol }));

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

// Crear pedido: llamada SÍNCRONA a InventoryService antes de confirmar.
app.MapPost("/orders", async (HttpRequest req) =>
{
    var input = OrderValidation.Validate(await Api.ReadJsonAsync(req));

    var started = Stopwatch.GetTimestamp();
    Availability availability;
    try
    {
        availability = await InventoryClients.WithRetry(
            () => inventory.CheckAvailabilityAsync(input.ProductId, input.Quantity), retries);
    }
    catch (InventoryException e) when (e.Code == InventoryErrorCode.NotFound)
    {
        throw new HttpError(404, e.Message);
    }
    catch (InventoryException e)
    {
        // Acoplamiento temporal: si Inventory no responde, no podemos confirmar el pedido.
        Console.Error.WriteLine($"[orders] No se pudo verificar stock: {e.Message}");
        return Api.Reply(503, new
        {
            error = "No se pudo verificar el stock: InventoryService no disponible. Intente más tarde.",
            detail = e.Message,
        });
    }
    var latencyMs = Math.Round(Stopwatch.GetElapsedTime(started).TotalMilliseconds, 2);

    var order = new Order(
        Guid.NewGuid().ToString(),
        input.ProductId,
        input.Quantity,
        input.CustomerEmail,
        availability.Name,
        availability.Available ? "CONFIRMADO" : "RECHAZADO",
        availability.Available ? null : $"Stock insuficiente (disponible: {availability.Stock})",
        new InventoryCheck(inventory.Protocol, latencyMs, availability.Stock),
        DateTime.UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'"));
    lock (ordersLock) orders[order.Id] = order;

    Console.WriteLine($"[orders] Pedido {order.Id} {order.Status} ({input.ProductId} x{input.Quantity}, {latencyMs} ms vía {inventory.Protocol})");
    return Api.Reply(availability.Available ? 201 : 409, order);
});

app.Lifetime.ApplicationStarted.Register(() =>
    Console.WriteLine($"OrdersService escuchando en http://0.0.0.0:{port} (inventario vía {inventory.Protocol.ToUpperInvariant()})"));
app.Run();
return 0;

record InventoryCheck(string Protocol, double LatencyMs, int Stock);

record Order(
    string Id,
    string ProductId,
    int Quantity,
    string CustomerEmail,
    string ProductName,
    string Status,
    string? Reason,
    InventoryCheck InventoryCheck,
    string CreatedAt);
