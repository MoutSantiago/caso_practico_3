using System.Text;
using System.Text.Json;
using RabbitMQ.Client;

// Publica eventos en RabbitMQ con "publisher confirms" (el broker confirma que guardó el mensaje).
// Si RabbitMQ no está disponible, los eventos quedan en una cola local (outbox simplificado
// en memoria) y se reenvían en orden cuando vuelve la conexión.
public class EventPublisher(string url)
{
    record Entry(string RoutingKey, OrderEvent Event, TaskCompletionSource<bool> Notify);

    readonly List<Entry> pending = [];
    readonly object gate = new();
    readonly TimeSpan ackTimeout = TimeSpan.FromMilliseconds(Api.EnvInt("PUBLISH_TIMEOUT_MS", 3000));
    volatile IModel? channel;
    int connecting;
    int flushing;

    public bool Connected => channel is not null;

    public int PendingCount
    {
        get { lock (gate) return pending.Count; }
    }

    public void Start()
    {
        _ = ConnectAsync();
        _ = Task.Run(async () =>
        {
            using var timer = new PeriodicTimer(TimeSpan.FromSeconds(3));
            while (await timer.WaitForNextTickAsync()) await FlushAsync();
        });
    }

    async Task ConnectAsync()
    {
        if (Interlocked.Exchange(ref connecting, 1) == 1) return;
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                var ch = await Task.Run(() =>
                {
                    var factory = new ConnectionFactory { Uri = new Uri(url), AutomaticRecoveryEnabled = false };
                    var connection = factory.CreateConnection();
                    connection.ConnectionShutdown += (_, _) =>
                    {
                        Console.Error.WriteLine("[amqp] Conexión cerrada. Reintentando...");
                        channel = null;
                        _ = Task.Delay(2000).ContinueWith(_ => ConnectAsync());
                    };
                    var model = connection.CreateModel();
                    model.ConfirmSelect();
                    model.ModelShutdown += (_, _) =>
                    {
                        channel = null;
                        try { connection.Close(); } catch { /* fuerza la reconexión completa */ }
                    };
                    Topology.Assert(model);
                    return model;
                });
                channel = ch;
                Interlocked.Exchange(ref connecting, 0);
                Console.WriteLine("[amqp] Conectado a RabbitMQ");
                _ = FlushAsync();
                return;
            }
            catch (Exception e)
            {
                var wait = Math.Min(1000 * attempt, 10_000);
                Console.Error.WriteLine($"[amqp] No se pudo conectar (intento {attempt}): {e.Message}. Reintento en {wait} ms");
                await Task.Delay(wait);
            }
        }
    }

    // Devuelve true si el broker confirmó el mensaje, false si quedó pendiente.
    // Los eventos se encolan siempre, para respetar el orden aunque haya pendientes.
    public Task<bool> PublishAsync(string routingKey, OrderEvent ev)
    {
        var notify = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (gate) pending.Add(new Entry(routingKey, ev, notify));
        if (channel is null)
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
        if (channel is null || Interlocked.Exchange(ref flushing, 1) == 1) return;
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
                var ch = channel;
                if (ch is null) break;
                await Task.Run(() => Send(ch, entry.RoutingKey, entry.Event));
                lock (gate) pending.RemoveAt(0);
                entry.Notify.TrySetResult(true);
                Console.WriteLine($"[amqp] Publicado {entry.Event.EventType} {entry.Event.OrderId} (routing key \"{entry.RoutingKey}\")");
            }
        }
        catch (Exception e)
        {
            Entry[] snapshot;
            lock (gate) snapshot = [.. pending];
            Console.Error.WriteLine($"[amqp] No se pudo publicar, {snapshot.Length} evento(s) pendiente(s): {e.Message}");
            foreach (var entry in snapshot) entry.Notify.TrySetResult(false);
        }
        finally
        {
            Interlocked.Exchange(ref flushing, 0);
        }
    }

    static void Send(IModel ch, string routingKey, OrderEvent ev)
    {
        var props = ch.CreateBasicProperties();
        props.Persistent = true; // deliveryMode 2: el mensaje se guarda en disco
        props.ContentType = "application/json";
        props.MessageId = ev.EventId;
        props.Type = ev.EventType;
        props.Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds());
        ch.BasicPublish(Topology.Exchange, routingKey, props, Encoding.UTF8.GetBytes(JsonSerializer.Serialize(ev, Api.JsonOptions)));
        ch.WaitForConfirmsOrDie(TimeSpan.FromSeconds(5)); // publisher confirm
    }
}
