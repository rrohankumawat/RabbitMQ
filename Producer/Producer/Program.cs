using System.Text;
using RabbitMQ.Client;

var factory = new ConnectionFactory { HostName = "localhost" };
await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

await channel.ExchangeDeclareAsync("notifications", ExchangeType.Fanout, durable: true);   // <-- exchange type

var messages = new[]
{
    ("", "Site maintenance in 5 minutes")
};

foreach (var (key, text) in messages)
{
    var body = Encoding.UTF8.GetBytes(text);
    await channel.BasicPublishAsync(exchange: "notifications", routingKey: key, body: body);   // <-- routing key
    Console.WriteLine($"Sent [{key}] {text}");
}

Console.WriteLine("Done.");
