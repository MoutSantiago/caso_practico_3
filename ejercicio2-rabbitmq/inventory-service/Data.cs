public record Product(string Id, string Name, int Stock);

public record Availability(string ProductId, string Name, int Requested, int Stock, bool Available);

// "Base de datos" en memoria del inventario.
public static class InventoryData
{
    public static readonly IReadOnlyDictionary<string, Product> Products = new Dictionary<string, Product>
    {
        ["P001"] = new("P001", "Teclado mecánico", 10),
        ["P002"] = new("P002", "Mouse inalámbrico", 0),
        ["P003"] = new("P003", "Monitor 27\"", 3),
    };

    public static Availability? CheckAvailability(string productId, int quantity)
    {
        if (!Products.TryGetValue(productId, out var product)) return null;
        return new Availability(product.Id, product.Name, quantity, product.Stock, product.Stock >= quantity);
    }
}
