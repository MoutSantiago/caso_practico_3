using RabbitMQ.Client;

// Topología de RabbitMQ. Productor y consumidor la declaran igual (las declaraciones son
// idempotentes). Así la cola existe aunque NotificationService nunca haya arrancado y
// los mensajes no se pierden.
//
//   OrdersService --(order.created)--> [exchange orders.events (topic)]
//                                           └──> [cola notifications.order-created] --> NotificationService
//                                                     └─ rechazados ──> [orders.events.dlx] ──> [cola ...dlq]
public static class Topology
{
    public const string Exchange = "orders.events";
    public const string RoutingKey = "order.created";
    public const string Queue = "notifications.order-created";
    public const string Dlx = "orders.events.dlx";
    public const string Dlq = "notifications.order-created.dlq";

    public static void Assert(IModel channel)
    {
        channel.ExchangeDeclare(Exchange, ExchangeType.Topic, durable: true);
        channel.ExchangeDeclare(Dlx, ExchangeType.Fanout, durable: true);
        channel.QueueDeclare(Dlq, durable: true, exclusive: false, autoDelete: false);
        channel.QueueBind(Dlq, Dlx, "");
        channel.QueueDeclare(Queue,
            durable: true, // la cola sobrevive a reinicios del broker
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object> { ["x-dead-letter-exchange"] = Dlx });
        channel.QueueBind(Queue, Exchange, RoutingKey);
    }
}
