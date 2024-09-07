using System.Text;
using Azure.Messaging.ServiceBus;
using MesagePublisher.Helpers;

string connectionString = "";
const string queueOrTopicName = "credit-analysis-notifications";
const string analysisId = "hyperscale_parallelism3";
long maxSizeInBytes = 1048576; // More info https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-quotas#messaging-quotas
int batchCounter = 1;
int messageCount = 0;
int messagesToSend = 5;

ServiceBusClient client;
ServiceBusSender sender;

(client, sender) = ServiceBusHelper.CreateSender(connectionString, queueOrTopicName);

var messageBatch = await ServiceBusHelper.CreateMessageBatch(sender, maxSizeInBytes);

const string filePath = "documents.csv";

using (var reader = new StreamReader(filePath))
{
    string? document = string.Empty;

    //skip header
    reader.ReadLine();

    while ((document = reader.ReadLine()) != null && messageCount < messagesToSend)
    {
        var groupId = document;
        var idempotencyKey = Guid.NewGuid().ToString()[0..25];

        byte[]? compressedBody = null;

        try
        {
            var messageByteArray = Encoding.UTF8.GetBytes(GetMessageBody(analysisId, groupId, idempotencyKey));
            compressedBody = MsgPackHelper.SerializeCompressed(messageByteArray);
        }
        catch (Exception exception)
        {
            Console.WriteLine($"Error to compress serialized body. Message: {exception.Message} ");
        }

        var message = BuildMessage(groupId, idempotencyKey, compressedBody);

        // try adding a message to the batch
        // if the size exceeds, send the accumulated batch and create a new one, until the number of messages to send ends
        if (!messageBatch.TryAddMessage(message))
        {
            try
            {
                // Use the producer client to send the batch of messages to the Service Bus queue
                await sender.SendMessagesAsync(messageBatch);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error while sending message to bus. Message: {e.Message}");
                throw;
            }

            Console.WriteLine($"Batch size exceeded at {messageCount} iteration. {messageBatch.Count} messages were sent. Creating a new batch...");
            messageBatch = await ServiceBusHelper.CreateMessageBatch(sender, maxSizeInBytes);
            batchCounter++;
        }
        messageCount++;
    }
}

// Send any remaining messages
await sender.SendMessagesAsync(messageBatch);

try
{
    Console.WriteLine($"A batch of {messageCount} messages has been published to bus.");
    Console.WriteLine($"{batchCounter} batches were created.");
}
finally
{
    await sender.DisposeAsync();
    await client.DisposeAsync();
    messageBatch.Dispose();
}

Console.WriteLine("Press any key to end the application");
Console.ReadKey();

static string GetMessageBody(string analysisId, string groupId, string idempotencyKey)
{
    return @$"{{
    ""Event"": {{
        ""EventName"": ""ConcessionAnalysis"",
        ""EventVersion"": 1,
        ""EventKey"": ""{idempotencyKey}"",
        ""Timestamp"": ""{DateTime.UtcNow}"",
        ""Type"": ""ConcessionAnalysisCompleted"",
        ""SagaProcessKey"": ""{analysisId}"",
        ""Metadata"": {{
            ""analysis_id"": ""{analysisId}"",
            ""group_id"": ""{groupId}"",
            ""documents"": [
                {{
                    ""number"": ""{groupId}"",
                    ""root"": ""{groupId}"",
                    ""type"": ""CPF""
                }}
            ],
            ""output"": {{
                ""limits"": {{
                    ""expires_at"": ""2026-12-01"",
                    ""global_limit"": {{
                        ""max_limit_amount"": 100000.0
                    }},
                    ""products"": {{
                        ""credit_card"": {{
                            ""max_limit_amount"": 0,
                            ""metadata"": {{
                                ""key"": ""value""
                            }}
                        }},
                        ""working_capital"": {{
                            ""max_limit_amount"": 50000,
                            ""metadata"": {{
                                ""key"": ""value""
                            }}
                        }}
                    }}
                }},
                ""rating"": {{
                    ""expires_at"": ""2026-12-01"",
                    ""value"": ""5.0"",
                    ""metadata"": {{
                        ""key"": ""value""
                    }}
                }}
            }}
        }},
        ""ActionName"": ""Completed"",
        ""EventSource"": ""MessagePublisherConsoleApp"",
        ""Label"": ""ConcessionAnalysisCompleted""
    }},
    ""Requester"": ""credit-risk-paramaters"",
    ""Timestamp"": ""{DateTime.UtcNow}"",
    ""CommandKey"": ""{Guid.NewGuid().ToString()[0..25]}"",
    ""SessionKey"": ""{groupId}"",
    ""ChannelKey"": ""NotificationsTopic"",
    ""IdempotencyKey"": ""{idempotencyKey}"",
    ""SagaProcessKey"": ""{analysisId}"",
    ""BatchProcessKey"": null,
    ""Result"": null,
    ""Id"": ""{groupId}"",
    ""ApplicationKey"": ""MessagePublisherConsoleApp"",
    ""UserEmail"": ""teste@stone.com.br"",
    ""ValidationResult"": null
    }}";
}

static ServiceBusMessage BuildMessage(string groupId, string idempotencyKey, byte[]? compressedBody)
{
    var message = new ServiceBusMessage(compressedBody)
    {
        MessageId = Guid.NewGuid().ToString(),
        SessionId = groupId,
        ScheduledEnqueueTime = DateTime.UtcNow,
        CorrelationId = idempotencyKey,
        Subject = "PublishPublicEvent",
        ContentType = "application/x-msgpack"
        //ContentType = "application/json"
    };
    message.ApplicationProperties.Add("SerializationType", (int)SerializationType.MsgPack);
    return message;
}