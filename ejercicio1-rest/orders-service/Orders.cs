using System.Text.Json;

public record OrderInput(string ProductId, int Quantity, string CustomerEmail);

public static class OrderValidation
{
    public static OrderInput Validate(JsonElement body)
    {
        var productId = GetString(body, "productId");
        if (string.IsNullOrEmpty(productId)) throw new HttpError(400, "productId es obligatorio");

        if (body.ValueKind != JsonValueKind.Object
            || !body.TryGetProperty("quantity", out var q)
            || q.ValueKind != JsonValueKind.Number
            || !q.TryGetInt32(out var quantity)
            || quantity <= 0)
            throw new HttpError(400, "quantity debe ser un entero positivo");

        var email = GetString(body, "customerEmail");
        if (email is null || !email.Contains('@')) throw new HttpError(400, "customerEmail debe ser un email válido");

        return new OrderInput(productId, quantity, email);
    }

    static string? GetString(JsonElement body, string name) =>
        body.ValueKind == JsonValueKind.Object
        && body.TryGetProperty(name, out var p)
        && p.ValueKind == JsonValueKind.String
            ? p.GetString()
            : null;
}
