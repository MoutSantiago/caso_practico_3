using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

// Publica los eventos de estado del pedido en un topic de Kafka.
// - key = orderId: todos los eventos de un mismo pedido van a la MISMA partición, así que
//   los consumidores los leen en orden (creado -> confirmado -> enviado).
// - Productor idempotente + acks=all: sin duplicados por reintentos y sin pérdida si el broker confirma.
// - Si Kafka no está disponible, los eventos quedan pendientes en memoria y se reenvían en orden.
public class EventPublisher(string brokers, string topic, int partitions = 3)
{
    record Entry(OrderEvent Event, TaskCompletionSource<bool> Notify);

    readonly List<Entry> pending = [];
    readonly object gate = new();
    readonly TimeSpan ackTimeout = TimeSpan.FromMilliseconds(Api.EnvInt("PUBLISH_TIMEOUT_MS", 3000));
    volatile IProducer<string, string>? producer;
    int flushing;

    public bool Ready => producer is not null;

    public int PendingCount
    {
        get { lock (gate) return pending.Count; }
    }

    public async Task StartAsync()
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await EnsureTopicAsync();
                producer = new ProducerBuilder<string, string>(new ProducerConfig
                {
                    BootstrapServers = brokers,
                    ClientId = "orders-service",
                    EnableIdempotence = true,
                    Acks = Acks.All, // espera a todas las réplicas sincronizadas
                    MaxInFlight = 1,
                    MessageTimeoutMs = 10_000,
                }).Build();
                Console.WriteLine($"[kafka] Productor conectado. Topic \"{topic}\" ({partitions} particiones)");
                _ = Task.Run(async () =>
                {
                    using var timer = new PeriodicTimer(TimeSpan.FromSeconds(3));
                    while (await timer.WaitForNextTickAsync()) await FlushAsync();
                });
                _ = FlushAsync();
                return;
            }
            catch (Exception e)
            {
                var wait = Math.Min(1000 * attempt, 10_000);
                Console.Error.WriteLine($"[kafka] No se pudo conectar (intento {attempt}): {e.Message}. Reintento en {wait} ms");
                await Task.Delay(wait);
            }
        }
    }

    async Task EnsureTopicAsync()
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = brokers }).Build();
        try
        {
            await admin.CreateTopicsAsync(
                [new TopicSpecification { Name = topic, NumPartitions = partitions, ReplicationFactor = 1 }],
                new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(10) });
            Console.WriteLine($"[kafka] Topic \"{topic}\" creado");
        }
        catch (CreateTopicsException e) when (e.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            // ya existía (lo crea kafka-init)
        }
    }

    // Devuelve true si Kafka confirmó la escritura, false si quedó pendiente.
    public Task<bool> PublishAsync(OrderEvent ev)
    {
        var notify = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (gate) pending.Add(new Entry(ev, notify));
        if (producer is null)
        {
            notify.TrySetResult(false);
            return notify.Task;
        }
        // No dejamos colgada la respuesta HTTP: si el broker tarda, el evento sigue pendiente.
        _ = Task.Delay(ackTimeout).ContinueWith(_ => notify.TrySetResult(false));
        _ = FlushAsync();
        return notify.Task;
    }

    async Task FlushAsync()
    {
        var p = producer;
        if (p is null || Interlocked.Exchange(ref flushing, 1) == 1) return;
        try
        {
            while (true)
            {
                Entry entry;
                lock (gate)
                {
                    if (pending.Count == 0) break;
                    entry = pending[0];
                }
                var result = await p.ProduceAsync(topic, new Message<string, string>
                {
                    Key = entry.Event.OrderId,
                    Value = JsonSerializer.Serialize(entry.Event, Api.JsonOptions),
                    Headers = new Headers { { "eventType", Encoding.UTF8.GetBytes(entry.Event.EventType) } },
                });
                lock (gate) pending.RemoveAt(0);
                entry.Notify.TrySetResult(true);
                Console.WriteLine(
                    $"[kafka] {entry.Event.EventType} pedido {entry.Event.OrderId[..8]} -> partición {result.Partition.Value}, offset {result.Offset.Value}");
            }
        }
        catch (Exception e)
        {
            Entry[] snapshot;
            lock (gate) snapshot = [.. pending];
            Console.Error.WriteLine($"[kafka] No se pudo publicar, {snapshot.Length} evento(s) pendiente(s): {e.Message}");
            foreach (var entry in snapshot) entry.Notify.TrySetResult(false);
        }
        finally
        {
            Interlocked.Exchange(ref flushing, 0);
        }
    }
}
