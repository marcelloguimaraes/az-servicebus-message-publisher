
using System.Text;
using Azure.Messaging.ServiceBus;

string connectionString = "Endpoint=sb://sb-finproducts-ec2-dev.servicebus.windows.net/;SharedAccessKeyName=default;SharedAccessKey=BN2D1vgOTD5jgKcjyqDx6eFzEOleKkwwu+ASbErEkGM=;EntityPath=commands-marcello";
const int messageCount = 50000;
const string queueName = "commands-marcello";
const string analysisId = "testeMarcelloFixJobs";
ServiceBusClient client;
ServiceBusSender sender;
var clientOptions = new ServiceBusClientOptions()
{ 
    TransportType = ServiceBusTransportType.AmqpWebSockets
};
client = new ServiceBusClient(connectionString, clientOptions);
sender = client.CreateSender(queueName);
int maxMessageSizeInKB = 256;
long maxSizeInBytes = maxMessageSizeInKB * 1024;
int batchCounter = 1;

var createMessageBatchOptions = new CreateMessageBatchOptions(){
    MaxSizeInBytes = maxSizeInBytes
};

ServiceBusMessageBatch messageBatch = await sender.CreateMessageBatchAsync(createMessageBatchOptions);

for (int i = 1; i <= messageCount; i++)
{
    var groupId = Guid.NewGuid().ToString("N");
    var messageBody = $@"{{""AnalysisId"":""{{{analysisId}}}"", ""GroupId"":""{{{groupId}}}"",""Documents"":[{{""Number"":""16418662091"",""Root"":""16418662091"",""Type"":1}}],""Limit"":{{""ExpiresAt"":""2023-12-01T00:00:00"",""GlobalLimit"":{{""MaxLimitAmount"":100000.0}},""ProductLimits"":[{{""Type"":0,""MaxLimitAmount"":50000.0,""Metadata"":{{""max_with_draw_amount"":""2000"",""max_withdraw_percentile"":""0.2""}}}},{{""Type"":1,""MaxLimitAmount"":50000.0,""Metadata"":{{""min_interest_rate"":""2.99"",""max_interest_rate"":""5.99"",""min_term_in_month"":""6"",""max_term_in_month"":""12"",""max_pmt"":""5000"",""estimated_monthly_revenue"":""1200300400""}}}}]}},""Rating"":{{""ExpiresAt"":""2023-12-01T00:00:00"",""Value"":9.9}},""Timestamp"":""2023-11-08T16:46:39.0348313Z"",""CommandKey"":""jqz1ga4p84hp5uqb5zrjtjnnx"",""SessionKey"":""18632572504"",""ChannelKey"":""CommandsQueue"",""IdempotencyKey"":""jqz1ga4p81rz73jvcgvv6xqcn"",""SagaProcessKey"":""jqz1ga4p81u6c87ftxfftnx73"",""BatchProcessKey"":null,""Result"":{{}},""Id"":""{{{groupId}}}"",""ApplicationKey"":""credit-postman"",""UserEmail"":""teste@stone.com.br"",""ValidationResult"":null}}";
    var message = new ServiceBusMessage(messageBody)
    {
        SessionId = groupId
    };

    System.Console.WriteLine(Encoding.UTF8.GetByteCount(message.Body.ToString()));

    // try adding a message to the batch
    // if the size exceeds, send the accumulated batch and create a new one, until the number of messages to send ends
    if (!messageBatch.TryAddMessage(message))
    {
        // Use the producer client to send the batch of messages to the Service Bus queue
        await sender.SendMessagesAsync(messageBatch);

        //Console.WriteLine($"Batch size exceeded at {i} iteration. {messageBatch.Count} messages were sent. Creating a new batch...");
        messageBatch = await sender.CreateMessageBatchAsync(createMessageBatchOptions);
        batchCounter++;
    }
}

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
