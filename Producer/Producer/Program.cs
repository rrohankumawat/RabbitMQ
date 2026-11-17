using RabbitMQ.Client;
using System.Text;

var factory = new ConnectionFactory
{
    HostName = "localhost"
};

using var conn = await factory.CreateConnectionAsync();

using var channel = await conn.CreateChannelAsync();

await channel.QueueDeclareAsync("hello", durable: true, exclusive: false, autoDelete: false);

var random = new Random();

for (int i = 0; i < 200; i++)
{
    var seconds = random.Next(1, 6);
    var msg = $"job-{i}: {seconds}";
    var props = new BasicProperties { Persistent = true };

    var body = Encoding.UTF8.GetBytes(msg);

    await channel.BasicPublishAsync(
        exchange: string.Empty,
        mandatory: false,
        basicProperties: props,
        body: body, 
        routingKey: "hello");
}

Console.WriteLine("Sent");