using System.Net.Mail;
using System.Text;

public static class Mailer
{
    // SMTP de pruebas: Mailpit captura los correos y los muestra en http://localhost:8025
    static readonly string Host = Environment.GetEnvironmentVariable("SMTP_HOST") ?? "localhost";
    static readonly int Port = int.TryParse(Environment.GetEnvironmentVariable("SMTP_PORT"), out var p) ? p : 1025;
    static readonly string From = Environment.GetEnvironmentVariable("MAIL_FROM") ?? "Tienda SD <no-reply@tienda-sd.local>";

    // Plantilla de correo según el tipo de evento.
    static (string Subject, string Body)? Template(OrderEvent ev)
    {
        var id = ev.OrderId[..8];
        var d = ev.Data;
        return ev.EventType switch
        {
            "OrderCreated" => ($"Recibimos tu pedido #{id}",
                $"Recibimos tu pedido de {d.Quantity} x {d.ProductName}. Estamos verificando el stock."),
            "OrderConfirmed" => ($"Tu pedido #{id} fue confirmado",
                $"¡Buenas noticias! Confirmamos tu pedido de {d.Quantity} x {d.ProductName}."),
            "OrderRejected" => ($"No pudimos confirmar tu pedido #{id}",
                $"Lo sentimos, no pudimos confirmar tu pedido de {d.ProductName}. Motivo: {d.Reason}."),
            "OrderShipped" => ($"Tu pedido #{id} está en camino",
                $"Despachamos tu pedido de {d.Quantity} x {d.ProductName}. Código de seguimiento: {d.TrackingCode}."),
            _ => null,
        };
    }

    // Devuelve false si el evento no requiere notificación.
    public static async Task<bool> SendOrderEmailAsync(OrderEvent ev)
    {
        if (Template(ev) is not var (subject, body)) return false;

        using var message = new MailMessage
        {
            From = new MailAddress(From),
            Subject = subject,
            SubjectEncoding = Encoding.UTF8,
            Body = $"¡Hola!\n\n{body}\n\nEstado actual: {ev.Status}",
            BodyEncoding = Encoding.UTF8,
        };
        message.To.Add(ev.Data.CustomerEmail);
        message.AlternateViews.Add(AlternateView.CreateAlternateViewFromString(
            $"<h2>{subject}</h2><p>{body}</p><p><small>Estado actual: <b>{ev.Status}</b></small></p>",
            Encoding.UTF8, "text/html"));

        using var smtp = new SmtpClient(Host, Port) { EnableSsl = false };
        await smtp.SendMailAsync(message);
        return true;
    }
}
