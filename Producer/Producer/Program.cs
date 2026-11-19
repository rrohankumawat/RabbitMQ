using System.Text;
using RabbitMQ.Client;

var factory = new ConnectionFactory { HostName = "localhost" };
await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

await channel.ExchangeDeclareAsync("reports", ExchangeType.Headers, durable: true);   // <-- exchange type

var messages = new[]
{
    ("Report A", "pdf", "low"),
    ("Report B", "csv", "low")
};

foreach (var (text, format, priority) in messages)
{
    var props = new BasicProperties
    {
        Headers = new Dictionary<string, object?> { ["format"] = format, ["priority"] = priority }   // <-- headers
    };
    var body = Encoding.UTF8.GetBytes(text);
    await channel.BasicPublishAsync(exchange: "reports", routingKey: "", mandatory: false,
        basicProperties: props, body: body);
    Console.WriteLine($"Sent {text} ({format}, {priority})");
}
