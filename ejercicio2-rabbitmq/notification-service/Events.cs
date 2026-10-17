using System.Text.Json;
using System.Text.Json.Serialization;

public record OrderEventData(string CustomerEmail, string ProductId, string ProductName, int Quantity);

public record OrderEvent(string EventId, string EventType, string OccurredAt, string OrderId, OrderEventData Data);

public static class Json
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };
}
