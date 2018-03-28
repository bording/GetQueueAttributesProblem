using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace GetQueueAttributesProblem
{
    class Program
    {
        static async Task Main(string[] args)
        {
            AWSConfigs.LoggingConfig.LogResponses = ResponseLoggingOption.Always;
            AWSConfigs.LoggingConfig.LogTo = LoggingOptions.Console;

            var client = new AmazonSQSClient();

            // Use any of these instead to see it work correctly
            //var client = new AmazonSQSClient(new AmazonSQSConfig { ServiceURL = "http://sqs.us-east-1.amazonaws.com" });
            //var client = new AmazonSQSClient(new AmazonSQSConfig { CacheHttpClient = false });
            //var client = new AmazonSQSClient(new AmazonSQSConfig { HttpClientCacheSize = 3 });

            var queueName1 = $"brandon-{Guid.NewGuid()}";
            var queueName2 = $"brandon-{Guid.NewGuid()}";
            var fifoQueueName = queueName2 + "-delay.fifo";

            var tasks = new List<Task>
            {
                CreateQueue(client, queueName1, false),
                CreateQueue(client, queueName2, true)
            };

            await Task.WhenAll(tasks);

            queueUrls.TryGetValue(fifoQueueName, out var queueUrl);

            var queueAttributes = await client.GetQueueAttributesAsync(queueUrl, new List<string> { "DelaySeconds", "MessageRetentionPeriod", "RedrivePolicy" });

            if (queueAttributes.DelaySeconds < 900)
            {
                throw new Exception();
            }
        }

        static async Task CreateQueue(AmazonSQSClient client, string queueName, bool fifo)
        {
            var createQueueRequest = new CreateQueueRequest
            {
                QueueName = queueName
            };

            var createQueueResponse = await client.CreateQueueAsync(createQueueRequest);

            queueUrls.TryAdd(queueName, createQueueResponse.QueueUrl);

            var setQueueAttributesRequest = new SetQueueAttributesRequest
            {
                QueueUrl = createQueueResponse.QueueUrl
            };

            setQueueAttributesRequest.Attributes.Add(QueueAttributeName.MessageRetentionPeriod, TimeSpan.FromDays(4).TotalSeconds.ToString(CultureInfo.InvariantCulture));

            await client.SetQueueAttributesAsync(setQueueAttributesRequest);

            if (fifo)
            {
                var fifoQueueName = queueName + "-delay.fifo";

                var createFifoQueueRequest = new CreateQueueRequest
                {
                    QueueName = fifoQueueName,
                    Attributes = new Dictionary<string, string> { { "FifoQueue", "true" } }
                };

                var createFifoQueueResponse = await client.CreateQueueAsync(createFifoQueueRequest);

                queueUrls.TryAdd(fifoQueueName, createFifoQueueResponse.QueueUrl);

                var setFifoQueueAttributesRequest = new SetQueueAttributesRequest
                {
                    QueueUrl = createFifoQueueResponse.QueueUrl
                };

                setFifoQueueAttributesRequest.Attributes.Add(QueueAttributeName.MessageRetentionPeriod, TimeSpan.FromDays(4).TotalSeconds.ToString(CultureInfo.InvariantCulture));
                setFifoQueueAttributesRequest.Attributes.Add(QueueAttributeName.DelaySeconds, 900.ToString(CultureInfo.InvariantCulture));

                await client.SetQueueAttributesAsync(setFifoQueueAttributesRequest);
            }
        }

        static readonly ConcurrentDictionary<string, string> queueUrls = new ConcurrentDictionary<string, string>();
    }
}
