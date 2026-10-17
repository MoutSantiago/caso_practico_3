using System.Text.Json;
using System.Text.Json.Serialization;

// Utilidades HTTP mínimas sobre ASP.NET Core (minimal APIs, sin controladores ni MVC).

public class HttpError(int status, string message) : Exception(message)
{
    public int Status { get; } = status;
}

public static class Api
{
    public static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public static int EnvInt(string name, int fallback) =>
        int.TryParse(Environment.GetEnvironmentVariable(name), out var value) ? value : fallback;

    public static IResult Reply(int status, object? body) => Results.Json(body, JsonOptions, statusCode: status);

    public static async Task<JsonElement> ReadJsonAsync(HttpRequest req, int limit = 1_000_000)
    {
        using var ms = new MemoryStream();
        var buffer = new byte[8192];
        int read;
        while ((read = await req.Body.ReadAsync(buffer)) > 0)
        {
            ms.Write(buffer, 0, read);
            if (ms.Length > limit) throw new HttpError(413, "Body demasiado grande");
        }
        if (ms.Length == 0) return JsonDocument.Parse("{}").RootElement;
        try
        {
            using var doc = JsonDocument.Parse(ms.ToArray());
            return doc.RootElement.Clone();
        }
        catch (JsonException)
        {
            throw new HttpError(400, "El body no es JSON válido");
        }
    }

    // Convierte las excepciones en respuestas JSON { error } y responde 404 a las rutas desconocidas.
    public static void UseJsonErrors(this WebApplication app)
    {
        app.Use(async (ctx, next) =>
        {
            try
            {
                await next();
            }
            catch (HttpError e)
            {
                await WriteError(ctx, e.Status, e.Message);
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e);
                await WriteError(ctx, 500, "Error interno");
            }
        });
        app.MapFallback(ctx =>
            WriteError(ctx, 404, $"Ruta no encontrada: {ctx.Request.Method} {ctx.Request.Path}"));
    }

    static Task WriteError(HttpContext ctx, int status, string message) =>
        Reply(status, new { error = message }).ExecuteAsync(ctx);
}
