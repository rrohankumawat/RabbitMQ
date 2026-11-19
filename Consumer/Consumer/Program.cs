using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory { HostName = "localhost" };
await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

await channel.ExchangeDeclareAsync("notifications", ExchangeType.Fanout, durable: true);   // same as the producer

var queueName = (await channel.QueueDeclareAsync(queue: "", durable: false,
    exclusive: true, autoDelete: true)).QueueName;   // temporary queue, one per subscriber

await channel.QueueBindAsync(queueName, "notifications", routingKey: "");   // <-- key is ignored by fanout

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += async (_, ea) =>
{
    var message = Encoding.UTF8.GetString(ea.Body.Span);
    Console.WriteLine($"Received [{ea.RoutingKey}] {message}");
    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
};

await channel.BasicConsumeAsync(queueName, autoAck: false, consumer: consumer);
Console.WriteLine("Waiting. Press Enter to exit.");
Console.ReadLine();
