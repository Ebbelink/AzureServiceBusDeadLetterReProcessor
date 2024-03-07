// See https://aka.ms/new-console-template for more information
using DeadLetterReProcessor;
using System.Net;

//Console.WriteLine("Let's process a dead letter queue.");
//Console.WriteLine("Please input the connection string");
//string connectionString = Console.ReadLine()!;
//Console.WriteLine("Please input the topic name");
//string topicName = Console.ReadLine()!;
//Console.WriteLine("Please input the subscriber name");
//string subscriberName = Console.ReadLine()!;

Console.WriteLine("Let's process a dead letter queue.");
string subscriptionName = "TODO";
string connectionString = "TODO";
string topicName = "TODO";

int fetchCount = 1000;

await TransferDeadLetterMessages.ProcessTopicAsync(connectionString, topicName, subscriptionName, fetchCount);