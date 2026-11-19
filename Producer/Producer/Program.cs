using System.Text;
using RabbitMQ.Client;

var factory = new ConnectionFactory { HostName = "localhost" };
await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

await channel.ExchangeDeclareAsync("events", ExchangeType.Topic, durable: true);   // <-- exchange type

var messages = new[]
{
    ("order.created.eu", "Order 1 placed in EU"),
    ("order.created.us", "Order 2 placed in US"),
    ("order.cancelled", "Order 3 cancelled")
};

foreach (var (key, text) in messages)
{
    var body = Encoding.UTF8.GetBytes(text);
    await channel.BasicPublishAsync(exchange: "events", routingKey: key, body: body);   // <-- routing key
    Console.WriteLine($"Sent [{key}] {text}");
}

Console.WriteLine("Done.");
