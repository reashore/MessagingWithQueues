
namespace Microsoft.Samples.MessagingWithQueues
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using ServiceBus;
    using ServiceBus.Messaging;

    public class Program
    {
        private static QueueClient _queueClient;
	    private const string QueueName = "SampleQueue";

        static void Main()
        {
            // Please see http://go.microsoft.com/fwlink/?LinkID=249089 for getting Service Bus connection string and adding to app.config

            Console.WriteLine("Creating a Queue");
            CreateQueue();
            Console.WriteLine("Press any key to send messages");
            Console.ReadKey();
            SendMessages();
            Console.WriteLine("Press any key to receive messages");
            Console.ReadKey();
            ReceiveMessages();
            Console.WriteLine("\nPress any key to exit");
            Console.ReadKey();
        }

        private static void CreateQueue()
        {
            NamespaceManager namespaceManager = NamespaceManager.Create();

            Console.WriteLine("\nCreating Queue '{0}'", QueueName);

            if (namespaceManager.QueueExists(QueueName))
            {
                namespaceManager.DeleteQueue(QueueName);
            }

            namespaceManager.CreateQueue(QueueName);
        }

        private static void SendMessages()
        {
            _queueClient = QueueClient.Create(QueueName);

	        List<BrokeredMessage> messageList = new List<BrokeredMessage>
	        {
		        CreateSampleMessage("1", "First message information"),
		        CreateSampleMessage("2", "Second message information"),
		        CreateSampleMessage("3", "Third message information")
	        };

	        Console.WriteLine("\nSending messages to Queue");

            foreach (BrokeredMessage message in messageList)
            {
                while (true)
                {
                    try
                    {
                        _queueClient.Send(message);
                    }
                    catch (MessagingException exception)
                    {
	                    if (!exception.IsTransient)
                        {
                            Console.WriteLine(exception.Message);
                            throw;
                        }

	                    HandleTransientErrors(exception);
                    }

	                Console.WriteLine("Message sent: Id = {0}, Body = {1}", message.MessageId, message.GetBody<string>());
                    break;
                }
            }
        }

        private static void ReceiveMessages()
        {
            Console.WriteLine("\nReceiving message from Queue...");

	        while (true)
            {
                try
                {
	                var message = _queueClient.Receive(TimeSpan.FromSeconds(5));
	                if (message != null)
                    {
                        Console.WriteLine("Message received: Id = {0}, Body = {1}", message.MessageId, message.GetBody<string>());
                        message.Complete();
                    }
                    else
                    {
                        break;
                    }
                }
                catch (MessagingException exception)
                {
	                if (!exception.IsTransient)
                    {
                        Console.WriteLine(exception.Message);
                        throw;
                    }

	                HandleTransientErrors(exception);
                }
            }

            _queueClient.Close();
        }

        private static BrokeredMessage CreateSampleMessage(string messageId, string messageBody)
        {
	        BrokeredMessage message = new BrokeredMessage(messageBody) {MessageId = messageId};
	        return message;
        }

        private static void HandleTransientErrors(MessagingException exception)
        {
            // If transient error/exception, retry after 2 seconds
            Console.WriteLine(exception.Message);
            Console.WriteLine("Will resend in 2 seconds");
            Thread.Sleep(2000);
        }
    }
}
