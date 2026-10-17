using Microsoft.AspNetCore.Server.Kestrel.Core;

var port = Api.EnvInt("PORT", 3001);
var latencyMs = Api.EnvInt("SIMULATED_LATENCY_MS", 0);

var builder = WebApplication.CreateBuilder(args);
builder.Logging.SetMinimumLevel(LogLevel.Warning);
builder.WebHost.ConfigureKestrel(o =>
{
    o.ListenAnyIP(port, l => l.Protocols = HttpProtocols.Http1);
    o.Limits.KeepAliveTimeout = TimeSpan.FromSeconds(65);
});

var app = builder.Build();
app.UseJsonErrors();

app.MapGet("/health", () => Api.Reply(200, new { status = "ok", service = "inventory-service" }));

app.MapGet("/inventory", () => Api.Reply(200, InventoryData.Products.Values));

app.MapGet("/inventory/{productId}", (string productId) =>
{
    if (!InventoryData.Products.TryGetValue(productId, out var product))
        throw new HttpError(404, $"Producto {productId} no existe");
    return Api.Reply(200, product);
});

// Endpoint que consulta OrdersService antes de confirmar un pedido.
app.MapGet("/inventory/{productId}/availability", async (string productId, HttpRequest req) =>
{
    if (latencyMs > 0) await Task.Delay(latencyMs); // para simular un servicio lento
    var quantity = 1;
    if (req.Query.TryGetValue("quantity", out var raw) && (!int.TryParse(raw, out quantity) || quantity <= 0))
        throw new HttpError(400, "quantity debe ser un entero positivo");

    var result = InventoryData.CheckAvailability(productId, quantity)
        ?? throw new HttpError(404, $"Producto {productId} no existe");
    Console.WriteLine($"[REST] availability {result.ProductId} x{quantity} -> {(result.Available ? "DISPONIBLE" : "SIN STOCK")}");
    return Api.Reply(200, result);
});

app.Lifetime.ApplicationStarted.Register(() =>
    Console.WriteLine($"[REST] InventoryService escuchando en http://0.0.0.0:{port}"));
app.Run();
