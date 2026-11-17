using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

var factory = new ConnectionFactory
{
    HostName = "localhost"
};

using var conn = await factory.CreateConnectionAsync();

using var channel = await conn.CreateChannelAsync();
await channel.QueueDeclareAsync("hello", durable: true, exclusive: false, autoDelete: false);

await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

var consume = new AsyncEventingBasicConsumer(channel);

consume.ReceivedAsync += async (sender, ea) =>
{
    var message = Encoding.UTF8.GetString(ea.Body.ToArray());

    Console.WriteLine(message);
    await Task.Delay(2000);
    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
};

await channel.BasicConsumeAsync("hello", autoAck: false, consume);

Console.WriteLine("Waiting. Press enter to exit");
Console.Read();