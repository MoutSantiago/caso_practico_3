// reason (rechazo) o trackingCode (envío) sólo se informan en el evento que corresponde.
public record OrderEventData(
    string CustomerEmail,
    string ProductId,
    string ProductName,
    int Quantity,
    string? Reason = null,
    string? TrackingCode = null);

public record OrderEvent(
    string EventId,
    string EventType,
    string OrderId,
    string Status,
    string? PreviousStatus,
    string OccurredAt,
    OrderEventData Data);
