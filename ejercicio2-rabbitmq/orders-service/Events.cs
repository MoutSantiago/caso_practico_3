public record OrderEventData(string CustomerEmail, string ProductId, string ProductName, int Quantity);

public record OrderEvent(string EventId, string EventType, string OccurredAt, string OrderId, OrderEventData Data);
