// See https://aka.ms/new-console-template for more information
using DeadLetterReProcessor;

//Console.WriteLine("Let's process a dead letter queue.");
//Console.WriteLine("Please input the connection string");
//string connectionString = Console.ReadLine()!;
//Console.WriteLine("Please input the topic name");
//string topicName = Console.ReadLine()!;
//Console.WriteLine("Please input the subscriber name");
//string subscriberName = Console.ReadLine()!;

Console.WriteLine("Let's process a dead letter queue.");
string connectionString = "TODO";
string subscriptionName = "TODO";
string topicName = "TODO";

int fetchCount = 100;

await DeadLetterProcessor.ProcessTopicAsync(connectionString, topicName, subscriptionName, fetchCount);