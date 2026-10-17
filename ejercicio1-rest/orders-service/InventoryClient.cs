using System.Text.Json;
using Grpc.Core;
using Grpc.Net.Client;
using InventoryContract;

// Clientes para consultar InventoryService. Ambos exponen la misma interfaz:
//   CheckAvailabilityAsync(productId, quantity) -> Availability
// y lanzan InventoryException con código NotFound o Unavailable.

public enum InventoryErrorCode { NotFound, Unavailable }

public class InventoryException(InventoryErrorCode code, string message) : Exception(message)
{
    public InventoryErrorCode Code { get; } = code;
}

public record Availability(string ProductId, string Name, int Requested, int Stock, bool Available);

public interface IInventoryClient
{
    string Protocol { get; }
    Task<Availability> CheckAvailabilityAsync(string productId, int quantity);
}

// ---------- REST (HTTP/1.1 + JSON) ----------
public class RestInventoryClient(string baseUrl, int timeoutMs) : IInventoryClient
{
    readonly HttpClient http = new(new SocketsHttpHandler { PooledConnectionLifetime = TimeSpan.FromMinutes(2) })
    {
        Timeout = Timeout.InfiniteTimeSpan,
    };

    public string Protocol => "rest";

    public async Task<Availability> CheckAvailabilityAsync(string productId, int quantity)
    {
        var url = $"{baseUrl}/inventory/{Uri.EscapeDataString(productId)}/availability?quantity={quantity}";
        using var cts = new CancellationTokenSource(timeoutMs);
        HttpResponseMessage res;
        string text;
        try
        {
            res = await http.GetAsync(url, cts.Token);
            text = await res.Content.ReadAsStringAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            throw new InventoryException(InventoryErrorCode.Unavailable, $"InventoryService no respondió (timeout de {timeoutMs} ms)");
        }
        catch (HttpRequestException e)
        {
            throw new InventoryException(InventoryErrorCode.Unavailable, $"InventoryService no respondió ({e.InnerException?.Message ?? e.Message})");
        }

        using (res)
        {
            if (res.StatusCode == System.Net.HttpStatusCode.NotFound)
                throw new InventoryException(InventoryErrorCode.NotFound, ErrorMessage(text) ?? $"Producto {productId} no existe");
            if (!res.IsSuccessStatusCode)
                throw new InventoryException(InventoryErrorCode.Unavailable, $"InventoryService respondió HTTP {(int)res.StatusCode}");
            return JsonSerializer.Deserialize<Availability>(text, Api.JsonOptions)!;
        }
    }

    static string? ErrorMessage(string body)
    {
        try
        {
            using var doc = JsonDocument.Parse(body);
            return doc.RootElement.TryGetProperty("error", out var e) ? e.GetString() : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }
}

// ---------- gRPC (HTTP/2 + Protocol Buffers) ----------
public class GrpcInventoryClient(string address, int timeoutMs) : IInventoryClient
{
    readonly InventoryContract.InventoryService.InventoryServiceClient client =
        new(GrpcChannel.ForAddress($"http://{address}"));

    public string Protocol => "grpc";

    public async Task<Availability> CheckAvailabilityAsync(string productId, int quantity)
    {
        try
        {
            var reply = await client.CheckAvailabilityAsync(
                new AvailabilityRequest { ProductId = productId, Quantity = quantity },
                deadline: DateTime.UtcNow.AddMilliseconds(timeoutMs));
            return new Availability(reply.ProductId, reply.Name, reply.Requested, reply.Stock, reply.Available);
        }
        catch (RpcException e) when (e.StatusCode == StatusCode.NotFound)
        {
            throw new InventoryException(InventoryErrorCode.NotFound, e.Status.Detail);
        }
        catch (RpcException e)
        {
            var name = e.StatusCode.ToString().ToUpperInvariant();
            throw new InventoryException(InventoryErrorCode.Unavailable, $"InventoryService (gRPC) {name}: {e.Status.Detail}");
        }
    }
}

public static class InventoryClients
{
    public static IInventoryClient Create()
    {
        var timeoutMs = Api.EnvInt("INVENTORY_TIMEOUT_MS", 2000);
        var protocol = (Environment.GetEnvironmentVariable("INVENTORY_PROTOCOL") ?? "rest").ToLowerInvariant();
        return protocol == "grpc"
            ? new GrpcInventoryClient(Environment.GetEnvironmentVariable("INVENTORY_GRPC_ADDR") ?? "localhost:50051", timeoutMs)
            : new RestInventoryClient(Environment.GetEnvironmentVariable("INVENTORY_REST_URL") ?? "http://localhost:3001", timeoutMs);
    }

    // Reintenta sólo errores transitorios (servicio caído / timeout), nunca un 404.
    public static async Task<T> WithRetry<T>(Func<Task<T>> fn, int retries = 2, int delayMs = 200)
    {
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                return await fn();
            }
            catch (InventoryException e) when (e.Code == InventoryErrorCode.Unavailable && attempt < retries)
            {
                var wait = delayMs * (1 << attempt);
                Console.Error.WriteLine($"[inventory] {e.Message}. Reintento {attempt + 1}/{retries} en {wait} ms");
                await Task.Delay(wait);
            }
        }
    }
}
