using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory { HostName = "localhost" };
await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

await channel.ExchangeDeclareAsync("reports", ExchangeType.Headers, durable: true);   // same as the producer
const string queueName = "pdf_queue";
await channel.QueueDeclareAsync(queueName, durable: true, exclusive: false, autoDelete: false);

await channel.QueueBindAsync(queueName, "reports", routingKey: "",
    arguments: new Dictionary<string, object?>   // <-- header rules
    {  //x-match -> all, any
        ["x-match"] = "any",
        ["format"] = "pdf",
        ["priority"] = "low"
    });

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
