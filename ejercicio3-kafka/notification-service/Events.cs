using System.Text.Json;
using System.Text.Json.Serialization;

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

public static class Json
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };
}
