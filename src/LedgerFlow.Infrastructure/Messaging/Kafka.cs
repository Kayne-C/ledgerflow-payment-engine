using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LedgerFlow.Infrastructure.Messaging;

internal static class KafkaHeaders
{
    public const string MessageId = "message-id";
    public const string MessageType = "message-type";
    public const string TraceParent = "traceparent";
    public const string Error = "error";

    public static string? Read(Headers headers, string key) =>
        headers.TryGetLastBytes(key, out var bytes) ? Encoding.UTF8.GetString(bytes) : null;
}

/// <summary>Idempotent producer (acks=all, enable.idempotence): broker-side de-duplication of producer retries, per-key ordering.</summary>
public sealed class KafkaMessageBus : IMessageBus, IDisposable
{
    private readonly Lazy<IProducer<string, byte[]>> _producer;

    public KafkaMessageBus(IOptions<MessagingOptions> options) =>
        _producer = new Lazy<IProducer<string, byte[]>>(() => new ProducerBuilder<string, byte[]>(new ProducerConfig
        {
            BootstrapServers = options.Value.Kafka.BootstrapServers,
            EnableIdempotence = true,
            Acks = Acks.All,
            LingerMs = 5,
            CompressionType = CompressionType.Lz4,
            ClientId = $"ledgerflow-relay-{Environment.MachineName}",
        }).Build());

    public async Task<IReadOnlyList<Exception?>> PublishAsync(IReadOnlyList<MessageEnvelope> envelopes, CancellationToken cancellationToken)
    {
        // All deliveries of a batch are in flight together; the producer batches them on the wire.
        var deliveries = envelopes.Select(envelope => _producer.Value.ProduceAsync(envelope.Topic, ToMessage(envelope), cancellationToken)).ToList();

        var results = new Exception?[envelopes.Count];
        for (var i = 0; i < deliveries.Count; i++)
        {
            try
            {
                await deliveries[i];
            }
            catch (ProduceException<string, byte[]> exception)
            {
                results[i] = exception;
            }
        }

        return results;
    }

    public void Dispose()
    {
        if (_producer.IsValueCreated)
        {
            _producer.Value.Flush(TimeSpan.FromSeconds(5));
            _producer.Value.Dispose();
        }
    }

    internal static Message<string, byte[]> ToMessage(MessageEnvelope envelope)
    {
        var headers = new Headers
        {
            { KafkaHeaders.MessageId, Encoding.UTF8.GetBytes(envelope.MessageId.ToString()) },
            { KafkaHeaders.MessageType, Encoding.UTF8.GetBytes(envelope.MessageType) },
        };

        if (envelope.TraceParent is not null)
        {
            headers.Add(KafkaHeaders.TraceParent, Encoding.UTF8.GetBytes(envelope.TraceParent));
        }

        return new Message<string, byte[]> { Key = envelope.PartitionKey, Value = Encoding.UTF8.GetBytes(envelope.Payload), Headers = headers };
    }
}

/// <summary>
/// N consumers in one group, each on its own thread. A partition is owned by exactly one consumer, and messages of a
/// partition are handled sequentially — together with keying saga commands by source account this serializes all
/// debits of an account (single writer) while thousands of accounts proceed in parallel. Offsets are stored only after
/// a message is handled (at-least-once); poison messages go to the dead-letter topic after a bounded number of attempts.
/// </summary>
public sealed partial class KafkaConsumerHost(
    MessageDispatcher dispatcher,
    IOptions<MessagingOptions> options,
    ILogger<KafkaConsumerHost> logger) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var settings = options.Value;
        if (settings.Transport != MessageTransport.Kafka || !settings.ConsumersEnabled)
        {
            return Task.CompletedTask;
        }

        var workers = Enumerable.Range(0, settings.Kafka.ConsumersPerProcess)
            .Select(index =>
            {
                var completion = new TaskCompletionSource();
                var thread = new Thread(() =>
                {
                    try
                    {
                        ConsumeLoop(index, settings.Kafka, stoppingToken);
                        completion.SetResult();
                    }
                    catch (Exception exception)
                    {
                        completion.SetException(exception);
                    }
                })
                {
                    IsBackground = true,
                    Name = $"kafka-consumer-{index}",
                };
                thread.Start();
                return completion.Task;
            })
            .ToList();

        return Task.WhenAll(workers);
    }

    private void ConsumeLoop(int index, KafkaOptions kafka, CancellationToken stoppingToken)
    {
        using var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            GroupId = kafka.ConsumerGroup,
            ClientId = $"ledgerflow-{Environment.MachineName}-{index}",
            EnableAutoCommit = true,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
        }).Build();

        using var deadLetters = new ProducerBuilder<string, byte[]>(new ProducerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            EnableIdempotence = true,
            Acks = Acks.All,
        }).Build();

        consumer.Subscribe(kafka.CommandsTopic);
        LogStarted(logger, index, kafka.CommandsTopic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var result = consumer.Consume(stoppingToken);
                if (result?.Message is null)
                {
                    continue;
                }

                HandleWithRetries(result, kafka, deadLetters, stoppingToken);
                consumer.StoreOffset(result);
            }
        }
        catch (OperationCanceledException)
        {
            // Shutdown.
        }
        finally
        {
            consumer.Close();
        }
    }

    private void HandleWithRetries(ConsumeResult<string, byte[]> result, KafkaOptions kafka, IProducer<string, byte[]> deadLetters, CancellationToken stoppingToken)
    {
        var envelope = new MessageEnvelope(
            Guid.TryParse(KafkaHeaders.Read(result.Message.Headers, KafkaHeaders.MessageId), out var id) ? id : Guid.Empty,
            result.Topic,
            result.Message.Key,
            KafkaHeaders.Read(result.Message.Headers, KafkaHeaders.MessageType) ?? string.Empty,
            Encoding.UTF8.GetString(result.Message.Value),
            KafkaHeaders.Read(result.Message.Headers, KafkaHeaders.TraceParent));

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                dispatcher.DispatchAsync(envelope, stoppingToken).GetAwaiter().GetResult();
                return;
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                if (attempt >= kafka.MaxDeliveryAttempts)
                {
                    LogDeadLettered(logger, exception, envelope.MessageType, envelope.MessageId, attempt);
                    var message = KafkaMessageBus.ToMessage(envelope);
                    message.Headers.Add(KafkaHeaders.Error, Encoding.UTF8.GetBytes(exception.Message));
                    deadLetters.ProduceAsync(kafka.DeadLetterTopic, message, stoppingToken).GetAwaiter().GetResult();
                    return;
                }

                LogRetrying(logger, exception, envelope.MessageType, envelope.MessageId, attempt);

                // Blocking the partition briefly preserves per-account ordering during a transient outage.
                Task.Delay(TimeSpan.FromMilliseconds(200 * Math.Pow(2, attempt - 1)), stoppingToken).GetAwaiter().GetResult();
            }
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Kafka consumer {Index} subscribed to {Topic}")]
    private static partial void LogStarted(ILogger logger, int index, string topic);

    [LoggerMessage(Level = LogLevel.Warning, Message = "{MessageType} {MessageId} failed (attempt {Attempt}); retrying")]
    private static partial void LogRetrying(ILogger logger, Exception exception, string messageType, Guid messageId, int attempt);

    [LoggerMessage(Level = LogLevel.Error, Message = "{MessageType} {MessageId} failed {Attempt} times; sent to the dead-letter topic")]
    private static partial void LogDeadLettered(ILogger logger, Exception exception, string messageType, Guid messageId, int attempt);
}

/// <summary>Creates topics idempotently at startup (partition count is the parallelism budget of the saga).</summary>
public sealed partial class KafkaTopologyInitializer(IOptions<MessagingOptions> options, ILogger<KafkaTopologyInitializer> logger) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var settings = options.Value;
        if (settings.Transport != MessageTransport.Kafka)
        {
            return;
        }

        var kafka = settings.Kafka;
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = kafka.BootstrapServers }).Build();
        var topics = new[]
        {
            new TopicSpecification { Name = kafka.CommandsTopic, NumPartitions = kafka.Partitions, ReplicationFactor = -1 },
            new TopicSpecification { Name = kafka.EventsTopic, NumPartitions = kafka.Partitions, ReplicationFactor = -1 },
            new TopicSpecification { Name = kafka.DeadLetterTopic, NumPartitions = 1, ReplicationFactor = -1 },
        };

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await admin.CreateTopicsAsync(topics);
                LogCreated(logger, string.Join(", ", topics.Select(t => t.Name)));
                return;
            }
            catch (CreateTopicsException exception) when (exception.Results.All(r => r.Error.Code is ErrorCode.NoError or ErrorCode.TopicAlreadyExists))
            {
                return;
            }
            catch (KafkaException exception) when (attempt < 30)
            {
                LogBrokerUnavailable(logger, exception, attempt);
                await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
            }
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    [LoggerMessage(Level = LogLevel.Information, Message = "Kafka topics ready: {Topics}")]
    private static partial void LogCreated(ILogger logger, string topics);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Kafka not reachable yet (attempt {Attempt})")]
    private static partial void LogBrokerUnavailable(ILogger logger, Exception exception, int attempt);
}

internal sealed class KafkaHealthCheck(IOptions<MessagingOptions> options) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var settings = options.Value;
        if (settings.Transport != MessageTransport.Kafka || (!settings.Outbox.Enabled && !settings.ConsumersEnabled))
        {
            return Task.FromResult(HealthCheckResult.Healthy("Kafka is not used by this host."));
        }

        try
        {
            using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = settings.Kafka.BootstrapServers }).Build();
            var metadata = admin.GetMetadata(TimeSpan.FromSeconds(3));
            return Task.FromResult(metadata.Brokers.Count > 0 ? HealthCheckResult.Healthy() : HealthCheckResult.Unhealthy("No brokers."));
        }
        catch (KafkaException exception)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy("Kafka is unreachable.", exception));
        }
    }
}
