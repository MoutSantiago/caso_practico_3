using System.Globalization;
using System.Text.Json;
using Confluent.Kafka;

var port = Api.EnvInt("PORT", 3003);
var topic = Environment.GetEnvironmentVariable("ORDER_EVENTS_TOPIC") ?? "order-events";
var groupId = Environment.GetEnvironmentVariable("KAFKA_GROUP_ID") ?? "analytics-service";

// ---------- Estado (proyección en memoria) ----------
var gate = new object();
var totalEvents = 0;
var eventsByType = new Dictionary<string, int>();
var eventsByPartition = new Dictionary<int, int>();
var unitsByProduct = new Dictionary<string, int>(); // unidades confirmadas por producto
string? firstEventAt = null;
string? lastEventAt = null;
var orders = new Dictionary<string, OrderView>(); // orderId -> estado + timeline

void Apply(OrderEvent ev, int partition, long offset)
{
    lock (gate)
    {
        totalEvents++;
        eventsByType[ev.EventType] = eventsByType.GetValueOrDefault(ev.EventType) + 1;
        eventsByPartition[partition] = eventsByPartition.GetValueOrDefault(partition) + 1;
        firstEventAt ??= ev.OccurredAt;
        lastEventAt = ev.OccurredAt;

        if (!orders.TryGetValue(ev.OrderId, out var order))
        {
            order = new OrderView
            {
                OrderId = ev.OrderId,
                ProductId = ev.Data.ProductId,
                Quantity = ev.Data.Quantity,
                Partition = partition,
            };
            orders[ev.OrderId] = order;
        }
        order.Status = ev.Status;
        order.Timeline.Add(new TimelineEntry(ev.Status, ev.OccurredAt, offset.ToString(CultureInfo.InvariantCulture)));

        if (ev.EventType == "OrderConfirmed")
            unitsByProduct[ev.Data.ProductId] = unitsByProduct.GetValueOrDefault(ev.Data.ProductId) + ev.Data.Quantity;
    }
}

static double SecondsBetween(string a, string b) =>
    (DateTimeOffset.Parse(b, CultureInfo.InvariantCulture) - DateTimeOffset.Parse(a, CultureInfo.InvariantCulture)).TotalSeconds;

object Summary()
{
    lock (gate)
    {
        var ordersByStatus = new Dictionary<string, int>();
        var createdToShipped = new List<double>();
        foreach (var o in orders.Values)
        {
            ordersByStatus[o.Status ?? ""] = ordersByStatus.GetValueOrDefault(o.Status ?? "") + 1;
            var created = o.Timeline.FirstOrDefault(t => t.Status == "CREADO");
            var shipped = o.Timeline.FirstOrDefault(t => t.Status == "ENVIADO");
            if (created is not null && shipped is not null) createdToShipped.Add(SecondsBetween(created.At, shipped.At));
        }
        var total = orders.Count;
        var rejected = ordersByStatus.GetValueOrDefault("RECHAZADO");
        double? avg = createdToShipped.Count > 0 ? Math.Round(createdToShipped.Average(), 2) : null;
        return new
        {
            totalEvents,
            eventsByType = new Dictionary<string, int>(eventsByType),
            eventsByPartition = new Dictionary<int, int>(eventsByPartition),
            unitsByProduct = new Dictionary<string, int>(unitsByProduct),
            firstEventAt,
            lastEventAt,
            totalOrders = total,
            ordersByStatus,
            rejectionRate = total > 0 ? Math.Round((double)rejected / total, 3) : 0,
            avgSecondsCreatedToShipped = avg,
        };
    }
}

// ---------- API HTTP ----------
var builder = WebApplication.CreateBuilder(args);
builder.Logging.SetMinimumLevel(LogLevel.Warning);
builder.WebHost.ConfigureKestrel(o => o.ListenAnyIP(port));
var app = builder.Build();
app.UseJsonErrors();

app.MapGet("/health", () => Api.Reply(200, new { status = "ok", service = "analytics-service" }));
app.MapGet("/stats", () => Api.Reply(200, Summary()));
app.MapGet("/stats/orders", () =>
{
    lock (gate) return Api.Reply(200, JsonSerializer.SerializeToNode(orders.Values.ToList(), Api.JsonOptions));
});
app.MapGet("/stats/orders/{id}", (string id) =>
{
    lock (gate)
    {
        if (!orders.TryGetValue(id, out var order)) throw new HttpError(404, "Pedido sin eventos registrados");
        return Api.Reply(200, JsonSerializer.SerializeToNode(order, Api.JsonOptions));
    }
});

// EnableAutoCommit = false -> este grupo nunca guarda offsets. Cada vez que arranca, relee el topic
// DESDE EL PRINCIPIO y reconstruye sus métricas en memoria (event sourcing / replay).
// Esto es posible porque Kafka conserva los eventos (retención), cosa que RabbitMQ no hace.
var consumerConfig = new ConsumerConfig
{
    BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BROKERS") ?? "localhost:9092",
    GroupId = groupId,
    ClientId = "analytics-service",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,
};

using var cts = new CancellationTokenSource();
app.Lifetime.ApplicationStopping.Register(cts.Cancel);

var consumerTask = Task.Run(() =>
{
    try
    {
        using var consumer = new ConsumerBuilder<string, string>(consumerConfig)
            .SetErrorHandler((_, e) => Console.Error.WriteLine($"[kafka] {e.Reason}"))
            .Build();
        consumer.Subscribe(topic);
        Console.WriteLine($"AnalyticsService consumiendo \"{topic}\" desde el inicio como grupo \"{groupId}\"");

        while (!cts.IsCancellationRequested)
        {
            ConsumeResult<string, string>? result;
            try
            {
                result = consumer.Consume(cts.Token);
            }
            catch (ConsumeException e)
            {
                Console.Error.WriteLine($"[kafka] {e.Error.Reason}");
                continue;
            }
            if (result?.Message is null) continue;

            var ev = JsonSerializer.Deserialize<OrderEvent>(result.Message.Value, Api.JsonOptions)!;
            Apply(ev, result.Partition.Value, result.Offset.Value);
            Console.WriteLine($"[analytics] p{result.Partition.Value}@{result.Offset.Value} {ev.EventType} pedido {ev.OrderId[..8]} (total eventos: {totalEvents})");
        }
        consumer.Close();
    }
    catch (OperationCanceledException)
    {
        // apagado ordenado
    }
    catch (Exception e)
    {
        Console.Error.WriteLine($"[kafka] El consumidor falló: {e.Message}");
        Environment.Exit(1); // Docker (restart: unless-stopped) lo vuelve a levantar
    }
});

app.Lifetime.ApplicationStarted.Register(() => Console.WriteLine($"AnalyticsService escuchando en http://0.0.0.0:{port}"));
app.Run();
await consumerTask;

record OrderEventData(string CustomerEmail, string ProductId, string ProductName, int Quantity, string? Reason = null, string? TrackingCode = null);

record OrderEvent(string EventId, string EventType, string OrderId, string Status, string? PreviousStatus, string OccurredAt, OrderEventData Data);

record TimelineEntry(string Status, string At, string Offset);

class OrderView
{
    public required string OrderId { get; init; }
    public required string ProductId { get; init; }
    public required int Quantity { get; init; }
    public required int Partition { get; init; }
    public List<TimelineEntry> Timeline { get; } = [];
    public string? Status { get; set; }
}
