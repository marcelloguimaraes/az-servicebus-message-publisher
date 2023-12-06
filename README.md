# Azure Service Bus Message Publisher
This is a program written in C# that sends messages in **batches** to Azure Service Bus. This program was created with performance in mind, allowing to send a large and configurable amount of messages in no time. 
Batches improves performance because it reduces the amount of networking round trips with the queue/topic as well as with reducing the async/await overhead in application side.

# How to run

1. Config queue/topic connection string, messageCount and messageSizeInBytes(according to the Pricing Tier)
```
string connectionString = "queue-or-topic";
const int messageCount = 50000;
const string queueName = "queue-name";
const string analysisId = "analisys-id";
long maxSizeInBytes = 1048576; //1MB for Premium tier, 256KB for Standard
// More info: https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-premium-messaging
```

2. Restore packages

```
dotnet restore
```

3. Run
```
dotnet run
```
