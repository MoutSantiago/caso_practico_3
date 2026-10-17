using Grpc.Core;
using InventoryContract;

// Servicio gRPC equivalente al endpoint REST /availability (para comparar ambos estilos).
public class InventoryGrpcService : InventoryContract.InventoryService.InventoryServiceBase
{
    static readonly int LatencyMs = Api.EnvInt("SIMULATED_LATENCY_MS", 0);

    public override async Task<AvailabilityReply> CheckAvailability(AvailabilityRequest request, ServerCallContext context)
    {
        if (LatencyMs > 0) await Task.Delay(LatencyMs);
        if (request.Quantity <= 0)
            throw new RpcException(new Status(StatusCode.InvalidArgument, "quantity debe ser un entero positivo"));

        var result = InventoryData.CheckAvailability(request.ProductId, request.Quantity);
        if (result is null)
            throw new RpcException(new Status(StatusCode.NotFound, $"Producto {request.ProductId} no existe"));

        Console.WriteLine($"[gRPC] CheckAvailability {request.ProductId} x{request.Quantity} -> {(result.Available ? "DISPONIBLE" : "SIN STOCK")}");
        return new AvailabilityReply
        {
            ProductId = result.ProductId,
            Name = result.Name,
            Requested = result.Requested,
            Stock = result.Stock,
            Available = result.Available,
        };
    }
}
