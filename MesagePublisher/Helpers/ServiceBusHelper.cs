using Azure.Messaging.ServiceBus;

namespace MesagePublisher.Helpers;

public static class ServiceBusHelper
{
    public static (ServiceBusClient client, ServiceBusSender sender) CreateSender(string connectionString, string queueOrTopicName)
    {
        var clientOptions = new ServiceBusClientOptions()
        {
            TransportType = ServiceBusTransportType.AmqpWebSockets
        };

        var client = new ServiceBusClient(connectionString, clientOptions);
        var sender = client.CreateSender(queueOrTopicName);

        return (client, sender);
    }

    public static Task<ServiceBusMessageBatch> CreateMessageBatch(ServiceBusSender sender, long? maxSizeInBytes)
    {
        var createMessageBatchOptions = new CreateMessageBatchOptions()
        {
            MaxSizeInBytes = maxSizeInBytes
        };

        return sender.CreateMessageBatchAsync(createMessageBatchOptions).AsTask();
    }
}