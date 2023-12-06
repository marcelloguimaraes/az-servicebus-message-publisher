using Azure.Messaging.ServiceBus;

string connectionString = "queue-or-topic";
const int messageCount = 50000;
const string queueName = "queue-name";
const string analysisId = "analisys-id";
long maxSizeInBytes = 1048576; //1MB
int batchCounter = 1;

ServiceBusClient client;
ServiceBusSender sender;

var clientOptions = new ServiceBusClientOptions()
{
    TransportType = ServiceBusTransportType.AmqpWebSockets
};

client = new ServiceBusClient(connectionString, clientOptions);
sender = client.CreateSender(queueName);

var createMessageBatchOptions = new CreateMessageBatchOptions()
{
    MaxSizeInBytes = maxSizeInBytes
};

var messageBatch = await sender.CreateMessageBatchAsync(createMessageBatchOptions);

for (int i = 1; i <= messageCount; i++)
{
    var groupId = Guid.NewGuid().ToString("N")[0..25];
    var messageBody = $@"{{""AnalysisId"":""{analysisId}"", ""GroupId"":""{groupId}"",""Documents"":[{{""Number"":""16418662091"",""Root"":""16418662091"",""Type"":1}}],""Limit"":{{""ExpiresAt"":""2024-01-01T00:00:00"",""GlobalLimit"":{{""MaxLimitAmount"":100000.0}},""ProductLimits"":[{{""Type"":0,""MaxLimitAmount"":50000.0,""Metadata"":{{""max_with_draw_amount"":""2000"",""max_withdraw_percentile"":""0.2""}}}},{{""Type"":1,""MaxLimitAmount"":50000.0,""Metadata"":{{""min_interest_rate"":""2.99"",""max_interest_rate"":""5.99"",""min_term_in_month"":""6"",""max_term_in_month"":""12"",""max_pmt"":""5000"",""estimated_monthly_revenue"":""1200300400""}}}}]}},""Rating"":{{""ExpiresAt"":""2024-01-01T00:00:00"",""Value"":9.9}},""Timestamp"":""2023-11-22T13:00:00.0348313Z"",""CommandKey"":""{Guid.NewGuid().ToString()[0..25]}"",""SessionKey"":""{groupId}"",""ChannelKey"":""CommandsQueue"",""IdempotencyKey"":""{Guid.NewGuid().ToString("N")[0..25]}"",""SagaProcessKey"":""{Guid.NewGuid().ToString("N")[0..25]}"",""BatchProcessKey"":null,""Result"":{{}},""Id"":""{groupId}"",""ApplicationKey"":""console-app"",""UserEmail"":""teste@stone.com.br"",""ValidationResult"":null}}";
    var message = new ServiceBusMessage(messageBody)
    {
        SessionId = groupId,
        Subject = "UpdateRiskParametersFromEngine",
        ContentType = "application/json"
    };

    // try adding a message to the batch
    // if the size exceeds, send the accumulated batch and create a new one, until the number of messages to send ends
    if (!messageBatch.TryAddMessage(message))
    {
        // Use the producer client to send the batch of messages to the Service Bus queue
        await sender.SendMessagesAsync(messageBatch);

        Console.WriteLine($"Batch size exceeded at {i} iteration. {messageBatch.Count} messages were sent. Creating a new batch...");
        messageBatch = await sender.CreateMessageBatchAsync(createMessageBatchOptions);
        batchCounter++;
    }
}

// Send any remaining messages
await sender.SendMessagesAsync(messageBatch);

try
{
    Console.WriteLine($"A batch of {messageCount} messages has been published to the queue.");
    Console.WriteLine($"{batchCounter} batches were created.");
}
finally
{
    // Calling DisposeAsync on client types is required to ensure that network
    // resources and other unmanaged objects are properly cleaned up.
    await sender.DisposeAsync();
    await client.DisposeAsync();
    messageBatch.Dispose();
}

Console.WriteLine("Press any key to end the application");
Console.ReadKey();