using System.Net.Mail;
using System.Text;

public static class Mailer
{
    // SMTP de pruebas: Mailpit captura los correos y los muestra en http://localhost:8025
    static readonly string Host = Environment.GetEnvironmentVariable("SMTP_HOST") ?? "localhost";
    static readonly int Port = int.TryParse(Environment.GetEnvironmentVariable("SMTP_PORT"), out var p) ? p : 1025;
    static readonly string From = Environment.GetEnvironmentVariable("MAIL_FROM") ?? "Tienda SD <no-reply@tienda-sd.local>";

    public static async Task SendOrderCreatedEmailAsync(OrderEvent ev)
    {
        var d = ev.Data;
        var shortId = ev.OrderId[..8];
        var text =
            $"¡Hola! Recibimos tu pedido #{shortId}.\n\n" +
            $"Producto: {d.ProductName} ({d.ProductId})\nCantidad: {d.Quantity}\n\n" +
            "Te vamos a avisar cuando sea despachado.";
        var html =
            "<h2>¡Gracias por tu compra!</h2>" +
            $"<p>Recibimos tu pedido <b>#{shortId}</b>.</p>" +
            $"<ul><li>Producto: {d.ProductName} ({d.ProductId})</li><li>Cantidad: {d.Quantity}</li></ul>" +
            "<p>Te vamos a avisar cuando sea despachado.</p>";

        using var message = new MailMessage
        {
            From = new MailAddress(From),
            Subject = $"Recibimos tu pedido #{shortId}",
            SubjectEncoding = Encoding.UTF8,
            Body = text,
            BodyEncoding = Encoding.UTF8,
        };
        message.To.Add(d.CustomerEmail);
        message.AlternateViews.Add(AlternateView.CreateAlternateViewFromString(html, Encoding.UTF8, "text/html"));

        using var smtp = new SmtpClient(Host, Port) { EnableSsl = false };
        await smtp.SendMailAsync(message);
    }
}
