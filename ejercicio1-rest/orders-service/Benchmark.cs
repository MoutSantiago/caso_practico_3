using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using Google.Protobuf;
using InventoryContract;

// Micro-benchmark REST vs gRPC para la misma consulta de disponibilidad.
// Uso (con el compose levantado):  docker compose exec orders-service dotnet OrdersService.dll benchmark
public static class Benchmark
{
    record Row(string Protocolo, double P50, double P95, double P99, long ReqPerSec);

    public static async Task<int> RunAsync()
    {
        var n = Api.EnvInt("BENCH_REQUESTS", 2000);
        var concurrency = Api.EnvInt("BENCH_CONCURRENCY", 50);
        var product = Environment.GetEnvironmentVariable("BENCH_PRODUCT") ?? "P001";

        var rest = new RestInventoryClient(Environment.GetEnvironmentVariable("INVENTORY_REST_URL") ?? "http://localhost:3001", 5000);
        var grpc = new GrpcInventoryClient(Environment.GetEnvironmentVariable("INVENTORY_GRPC_ADDR") ?? "localhost:50051", 5000);

        try
        {
            Console.WriteLine($"Benchmark: {n} llamadas por protocolo, producto {product}\n");
            var rows = new[]
            {
                await Run("REST/JSON", rest, n, concurrency, product),
                await Run("gRPC/Protobuf", grpc, n, concurrency, product),
            };

            var col = $"req/s (conc={concurrency})";
            Console.WriteLine($"{"protocolo",-15}{"p50 (ms)",10}{"p95 (ms)",10}{"p99 (ms)",10}{col,20}");
            foreach (var r in rows)
                Console.WriteLine($"{r.Protocolo,-15}{r.P50.ToString("F3", CultureInfo.InvariantCulture),10}{r.P95.ToString("F3", CultureInfo.InvariantCulture),10}{r.P99.ToString("F3", CultureInfo.InvariantCulture),10}{r.ReqPerSec,20}");

            // Tamaño del cuerpo de la respuesta (sin contar headers HTTP).
            var sample = await rest.CheckAvailabilityAsync(product, 1);
            var jsonBytes = JsonSerializer.SerializeToUtf8Bytes(sample, Api.JsonOptions).Length;
            var protoBytes = new AvailabilityReply
            {
                ProductId = sample.ProductId,
                Name = sample.Name,
                Requested = sample.Requested,
                Stock = sample.Stock,
                Available = sample.Available,
            }.CalculateSize();
            Console.WriteLine($"Tamaño del payload de respuesta: JSON = {jsonBytes} bytes | Protobuf = {protoBytes} bytes");
            return 0;
        }
        catch (Exception e)
        {
            Console.Error.WriteLine($"Error en el benchmark: {e.Message}");
            return 1;
        }
    }

    static double Percentile(List<double> sorted, int p) =>
        sorted[Math.Min(sorted.Count - 1, (int)Math.Floor(p / 100.0 * sorted.Count))];

    static async Task<Row> Run(string name, IInventoryClient client, int n, int concurrency, string product)
    {
        for (var i = 0; i < 50; i++) await client.CheckAvailabilityAsync(product, 1); // warm-up

        // 1) Secuencial: latencia de una llamada aislada
        var latencies = new List<double>(n);
        for (var i = 0; i < n; i++)
        {
            var t = Stopwatch.GetTimestamp();
            await client.CheckAvailabilityAsync(product, 1);
            latencies.Add(Stopwatch.GetElapsedTime(t).TotalMilliseconds);
        }
        latencies.Sort();

        // 2) Concurrente: throughput con varias llamadas en vuelo
        var next = -1;
        var t0 = Stopwatch.GetTimestamp();
        await Task.WhenAll(Enumerable.Range(0, concurrency).Select(async _ =>
        {
            while (Interlocked.Increment(ref next) < n) await client.CheckAvailabilityAsync(product, 1);
        }));
        var seconds = Stopwatch.GetElapsedTime(t0).TotalSeconds;

        return new Row(name, Percentile(latencies, 50), Percentile(latencies, 95), Percentile(latencies, 99), (long)Math.Round(n / seconds));
    }
}
