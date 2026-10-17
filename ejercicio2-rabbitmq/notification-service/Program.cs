using System.Collections.Concurrent;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var rabbitUrl = Environment.GetEnvironmentVariable("RABBITMQ_URL") ?? "amqp://admin:admin@localhost:5672";
var prefetch = int.TryParse(Environment.GetEnvironmentVariable("PREFETCH"), out var pf) ? pf : 5;
// Para la demo: simula un envío lento y permite ver cómo se acumulan los mensajes.
var processingDelayMs = int.TryParse(Environment.GetEnvironmentVariable("PROCESSING_DELAY_MS"), out var pd) ? pd : 0;

// Idempotencia: RabbitMQ garantiza entrega "al menos una vez", así que un mensaje puede
// llegar repetido (por ejemplo, si el servicio se cae después de enviar el mail y antes del ack).
var processed = new ConcurrentDictionary<string, byte>();
var shuttingDown = false;

var factory = new ConnectionFactory
{
    Uri = new Uri(rabbitUrl),
    DispatchConsumersAsync = true,
    AutomaticRecoveryEnabled = false,
};

try
{
    var connection = await ConnectWithRetry();
    connection.CallbackException += (_, e) => Console.Error.WriteLine($"[amqp] Error de conexión: {e.Exception.Message}");
    connection.ConnectionShutdown += (_, _) =>
    {
        if (shuttingDown) return;
        // Docker (restart: unless-stopped) vuelve a levantar el proceso y se reconecta.
        Console.Error.WriteLine("[amqp] Conexión cerrada. Saliendo para reiniciar.");
        Environment.Exit(1);
    };

    var channel = connection.CreateModel();
    Topology.Assert(channel);
    channel.BasicQos(0, (ushort)prefetch, false); // máximo de mensajes sin ack en vuelo
    var consumer = new AsyncEventingBasicConsumer(channel);
    consumer.Received += (_, ea) => HandleMessage(channel, ea);
    channel.BasicConsume(Topology.Queue, autoAck: false, consumer);
    Console.WriteLine($"NotificationService escuchando la cola \"{Topology.Queue}\"");

    var stop = new TaskCompletionSource();
    PosixSignalRegistration.Create(PosixSignal.SIGTERM, ctx => { ctx.Cancel = true; stop.TrySetResult(); });
    Console.CancelKeyPress += (_, e) => { e.Cancel = true; stop.TrySetResult(); };
    await stop.Task;

    Console.WriteLine("Cerrando NotificationService...");
    shuttingDown = true;
    try { connection.Close(); } catch { /* ya cerrada */ }
}
catch (Exception e)
{
    Console.Error.WriteLine($"Error fatal: {e}");
    return 1;
}
return 0;

async Task<IConnection> ConnectWithRetry()
{
    for (var attempt = 1; ; attempt++)
    {
        try
        {
            return factory.CreateConnection();
        }
        catch (Exception e)
        {
            var wait = Math.Min(1000 * attempt, 10_000);
            Console.Error.WriteLine($"[amqp] No se pudo conectar (intento {attempt}): {e.Message}. Reintento en {wait} ms");
            await Task.Delay(wait);
        }
    }
}

async Task HandleMessage(IModel channel, BasicDeliverEventArgs msg)
{
    OrderEvent? ev;
    try
    {
        ev = JsonSerializer.Deserialize<OrderEvent>(Encoding.UTF8.GetString(msg.Body.Span), Json.Options);
        if (ev is null) throw new JsonException("null");
    }
    catch (JsonException)
    {
        Console.Error.WriteLine("[notification] Mensaje con formato inválido -> DLQ");
        channel.BasicNack(msg.DeliveryTag, false, false);
        return;
    }

    if (processed.ContainsKey(ev.EventId))
    {
        Console.WriteLine($"[notification] Evento {ev.EventId} duplicado, se ignora");
        channel.BasicAck(msg.DeliveryTag, false);
        return;
    }

    var delaySec = (DateTimeOffset.UtcNow - DateTimeOffset.Parse(ev.OccurredAt, CultureInfo.InvariantCulture)).TotalSeconds;
    Console.WriteLine($"[notification] Recibido {ev.EventType} del pedido {ev.OrderId} (publicado hace {delaySec.ToString("F1", CultureInfo.InvariantCulture)} s)");

    try
    {
        if (processingDelayMs > 0) await Task.Delay(processingDelayMs);
        await Mailer.SendOrderCreatedEmailAsync(ev);
        processed[ev.EventId] = 0;
        channel.BasicAck(msg.DeliveryTag, false); // recién ahora RabbitMQ borra el mensaje de la cola
        Console.WriteLine($"[notification] Email enviado a {ev.Data.CustomerEmail}");
    }
    catch (Exception e)
    {
        // Primer fallo: se devuelve a la cola para reintentar. Si ya era un reintento: a la DLQ.
        var requeue = !msg.Redelivered;
        Console.Error.WriteLine($"[notification] Falló el envío ({e.Message}) -> {(requeue ? "reencolado" : "DLQ")}");
        if (requeue) await Task.Delay(2000);
        channel.BasicNack(msg.DeliveryTag, false, requeue);
    }
}
