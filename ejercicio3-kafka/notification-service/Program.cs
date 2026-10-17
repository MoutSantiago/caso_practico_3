using System.Globalization;
using System.Runtime.InteropServices;
using System.Text.Json;
using Confluent.Kafka;

var topic = Environment.GetEnvironmentVariable("ORDER_EVENTS_TOPIC") ?? "order-events";
var groupId = Environment.GetEnvironmentVariable("KAFKA_GROUP_ID") ?? "notification-service";

// Consumer group propio: recibe TODOS los eventos del topic, independientemente de
// lo que lea AnalyticsService (que está en otro grupo).
var config = new ConsumerConfig
{
    BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BROKERS") ?? "localhost:9092",
    GroupId = groupId,
    ClientId = "notification-service",
    // AutoOffsetReset sólo aplica la primera vez que el grupo se conecta (no tiene offsets guardados).
    // Después, Kafka recuerda el último offset confirmado del grupo y sigue desde ahí.
    AutoOffsetReset = AutoOffsetReset.Earliest,
    // El offset se guarda sólo cuando el mensaje se procesó sin error (StoreOffset más abajo);
    // el commit hacia Kafka es automático y periódico.
    EnableAutoCommit = true,
    EnableAutoOffsetStore = false,
};

// Idempotencia: Kafka entrega "al menos una vez". Si el servicio se cae después de enviar
// el mail y antes de confirmar el offset, el evento se vuelve a leer.
var processed = new HashSet<string>();

using var cts = new CancellationTokenSource();
PosixSignalRegistration.Create(PosixSignal.SIGTERM, ctx => { ctx.Cancel = true; cts.Cancel(); });
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

try
{
    using var consumer = new ConsumerBuilder<string, string>(config)
        .SetErrorHandler((_, e) => Console.Error.WriteLine($"[kafka] {e.Reason}"))
        .Build();
    consumer.Subscribe(topic);
    Console.WriteLine($"NotificationService consumiendo \"{topic}\" como grupo \"{groupId}\"");

    while (!cts.IsCancellationRequested)
    {
        ConsumeResult<string, string>? result;
        try
        {
            result = consumer.Consume(cts.Token);
        }
        catch (OperationCanceledException)
        {
            break;
        }
        catch (ConsumeException e)
        {
            Console.Error.WriteLine($"[kafka] {e.Error.Reason}");
            continue;
        }
        if (result?.Message is null) continue;

        try
        {
            await Handle(result);
            consumer.StoreOffset(result);
        }
        catch (Exception e)
        {
            // Si el SMTP falla, se vuelve a leer el mismo mensaje (no se pierde).
            Console.Error.WriteLine($"[notification] Falló el procesamiento ({e.Message}). Reintentando...");
            await Task.Delay(2000);
            consumer.Seek(result.TopicPartitionOffset);
        }
    }

    Console.WriteLine("Cerrando NotificationService...");
    consumer.Close();
}
catch (Exception e)
{
    Console.Error.WriteLine($"Error fatal: {e.Message}");
    return 1; // Docker (restart: unless-stopped) lo vuelve a levantar
}
return 0;

async Task Handle(ConsumeResult<string, string> result)
{
    var ev = JsonSerializer.Deserialize<OrderEvent>(result.Message.Value, Json.Options)!;
    if (processed.Contains(ev.EventId)) return;

    var delaySec = (DateTimeOffset.UtcNow - DateTimeOffset.Parse(ev.OccurredAt, CultureInfo.InvariantCulture)).TotalSeconds;
    var sent = await Mailer.SendOrderEmailAsync(ev);
    processed.Add(ev.EventId);
    Console.WriteLine(
        $"[notification] p{result.Partition.Value}@{result.Offset.Value} {ev.EventType} pedido {ev.OrderId[..8]}" +
        (sent ? $" -> mail a {ev.Data.CustomerEmail}" : " (sin mail)") +
        $" (publicado hace {delaySec.ToString("F1", CultureInfo.InvariantCulture)} s)");
}
