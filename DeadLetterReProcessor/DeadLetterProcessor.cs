using Azure.Messaging.ServiceBus;

namespace DeadLetterReProcessor;

public class DeadLetterProcessor
{
    // https://github.com/Azure/azure-sdk-for-net/blob/Azure.Messaging.ServiceBus_7.2.1/sdk/servicebus/Azure.Messaging.ServiceBus/README.md

    private static ServiceBusClient client;
    private static ServiceBusSender sender;

    public static async Task ProcessTopicAsync(string connectionString, string topicName, string subscriptionName, int fetchCount = 10)
    {
        try
        {
            client = new ServiceBusClient(connectionString);
            sender = client.CreateSender(topicName);

            ServiceBusReceiver dlqReceiver = client.CreateReceiver(topicName, subscriptionName, new ServiceBusReceiverOptions
            {
                SubQueue = SubQueue.DeadLetter,
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
                PrefetchCount = fetchCount
            });

            await ProcessDeadLetterMessagesAsync($"topic: {topicName} -> subscriber: {subscriptionName}", fetchCount, sender, dlqReceiver);
        }
        catch (ServiceBusException ex)
        {
            if (ex.Reason == Azure.Messaging.ServiceBus.ServiceBusFailureReason.MessagingEntityNotFound)
            {
                WriteError(ex);
                WriteError($"Topic:Subscriber '{topicName}:{subscriptionName}' not found. Check that the name provided is correct.");
            }
            else
            {
                throw;
            }
        }
        finally
        {
            await sender.CloseAsync();
            await client.DisposeAsync();
        }
    }

    public static async Task ProcessQueueAsync(string connectionString, string queueName, int fetchCount = 10)
    {
        try
        {
            client = new ServiceBusClient(connectionString);
            sender = client.CreateSender(queueName);

            ServiceBusReceiver dlqReceiver = client.CreateReceiver(queueName, new ServiceBusReceiverOptions
            {
                SubQueue = SubQueue.DeadLetter,
                ReceiveMode = ServiceBusReceiveMode.PeekLock
            });

            await ProcessDeadLetterMessagesAsync($"queue: {queueName}", fetchCount, sender, dlqReceiver);
        }
        catch (ServiceBusException ex)
        {
            if (ex.Reason == Azure.Messaging.ServiceBus.ServiceBusFailureReason.MessagingEntityNotFound)
            {
                WriteError(ex);
                WriteError($"Queue '{queueName}' not found. Check that the name provided is correct.");
            }
            else
            {
                throw;
            }
        }
        finally
        {
            await sender.CloseAsync();
            await client.DisposeAsync();
        }
    }

    private static async Task ProcessDeadLetterMessagesAsync(string source, int fetchCount, ServiceBusSender sender, ServiceBusReceiver dlqReceiver)
    {
        var wait = new TimeSpan(0, 0, 10);

        Console.WriteLine($"fetching messages ({wait.TotalSeconds} seconds retrieval timeout)");
        Console.WriteLine(source);

        int totalProcessed = 0;
        do
        {
            await Task.Delay(1000);
            IReadOnlyList<ServiceBusReceivedMessage> dlqMessages = await dlqReceiver.ReceiveMessagesAsync(fetchCount, wait);

            Console.WriteLine($"dl-count: {dlqMessages.Count}");

            int i = 1;

            foreach (var dlqMessage in dlqMessages)
            {
                //Console.WriteLine($"start processing message {i}");
                Console.WriteLine($"dl-message-dead-letter-message-id: {dlqMessage.MessageId}");
                //Console.WriteLine($"dl-message-dead-letter-reason: {dlqMessage.DeadLetterReason}");
                //Console.WriteLine($"dl-message-dead-letter-error-description: {dlqMessage.DeadLetterErrorDescription}");

                string cleanBody = "";
                if (!string.IsNullOrEmpty(dlqMessage.Body.ToString()))
                {
                    string stringVal = dlqMessage.Body.ToString();

                    int firstIndex = stringVal.IndexOf('{');
                    int lastIndex = stringVal.LastIndexOf('}');
                    cleanBody = stringVal.Substring(firstIndex, lastIndex - firstIndex + 1);

                }
                var resubmittableMessage = new ServiceBusMessage(dlqMessage);
                resubmittableMessage.Body = new BinaryData(cleanBody);

                await sender.SendMessageAsync(resubmittableMessage);

                try
                {
                    await dlqReceiver.CompleteMessageAsync(dlqMessage);
                }
                catch (ServiceBusException e)
                {
                    WriteError(resubmittableMessage.MessageId);
                    WriteError(e.Message);
                }


                Console.WriteLine($"finished processing message {i}");
                Console.WriteLine("--------------------------------------------------------------------------------------");

                i++;
                totalProcessed++;
            }
        } while (totalProcessed < fetchCount);

        await dlqReceiver.CloseAsync();

        Console.WriteLine($"finished");
    }

    private static void WriteError(string message)
    {
        var originalColor = Console.ForegroundColor;
        Console.ForegroundColor = ConsoleColor.Red;

        Console.Error.WriteLine(message);

        Console.ForegroundColor = originalColor;
    }

    private static void WriteError(object message)
    {
        var originalColor = Console.ForegroundColor;
        Console.ForegroundColor = ConsoleColor.Red;

        Console.Error.WriteLine(message);

        Console.ForegroundColor = originalColor;
    }
}
