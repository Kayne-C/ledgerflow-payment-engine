using System.Diagnostics;
using System.Text.Json;
using LedgerFlow.Application.Diagnostics;
using LedgerFlow.Application.Messaging;
using LedgerFlow.Infrastructure.EventStore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace LedgerFlow.Infrastructure.Messaging;

public enum MessageTransport
{
    /// <summary>In-process dispatch: local development and integration tests.</summary>
    InMemory,
    Kafka,
}

public sealed class MessagingOptions
{
    public const string Section = "Messaging";

    public MessageTransport Transport { get; set; } = MessageTransport.InMemory;

    public bool ConsumersEnabled { get; set; }

    public KafkaOptions Kafka { get; set; } = new();

    public OutboxRelayOptions Outbox { get; set; } = new();

    public SweeperOptions Sweeper { get; set; } = new();
}

public sealed class KafkaOptions
{
    public string BootstrapServers { get; set; } = "localhost:9092";

    public string CommandsTopic { get; set; } = "ledgerflow.payment-commands";

    public string EventsTopic { get; set; } = "ledgerflow.payment-events";

    public string DeadLetterTopic { get; set; } = "ledgerflow.payment-commands.dlq";

    public string ConsumerGroup { get; set; } = "ledgerflow-processor";

    /// <summary>Partitions of the commands topic = upper bound of parallel single-writer lanes.</summary>
    public int Partitions { get; set; } = 12;

    /// <summary>Consumer instances per process (each owns a subset of partitions).</summary>
    public int ConsumersPerProcess { get; set; } = 4;

    public int MaxDeliveryAttempts { get; set; } = 5;
}

public sealed class OutboxRelayOptions
{
    public bool Enabled { get; set; } = true;

    public int BatchSize { get; set; } = 500;

    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromMilliseconds(100);

    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(30);

    public int MaxAttempts { get; set; } = 20;
}

public sealed class SweeperOptions
{
    public bool Enabled { get; set; }

    public TimeSpan Interval { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan StalledFor { get; set; } = TimeSpan.FromMinutes(2);

    public TimeSpan HardTimeout { get; set; } = TimeSpan.FromMinutes(30);
}

/// <summary>Transport-neutral message as it travels from the outbox to a handler.</summary>
public sealed record MessageEnvelope(Guid MessageId, string Topic, string PartitionKey, string MessageType, string Payload, string? TraceParent);

public interface IMessageBus
{
    /// <returns>For each envelope (same order): null on success, otherwise the error.</returns>
    Task<IReadOnlyList<Exception?>> PublishAsync(IReadOnlyList<MessageEnvelope> envelopes, CancellationToken cancellationToken);
}

/// <summary>Runs every subscription of one message in a fresh DI scope, continuing the producer's trace.</summary>
public sealed partial class MessageDispatcher(IServiceScopeFactory scopeFactory, MessageCatalog catalog, ILogger<MessageDispatcher> logger)
{
    public async Task DispatchAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        var messageType = MessageCatalog.Resolve(envelope.MessageType);
        if (messageType is null)
        {
            LogUnknown(logger, envelope.MessageType, envelope.MessageId);
            return;
        }

        ActivityContext.TryParse(envelope.TraceParent, null, out var parent);
        using var activity = LedgerTelemetry.ActivitySource.StartActivity($"{envelope.MessageType} process", ActivityKind.Consumer, parent);
        activity?.SetTag("messaging.message.id", envelope.MessageId);
        activity?.SetTag("messaging.destination.name", envelope.Topic);

        var message = (IIntegrationMessage)JsonSerializer.Deserialize(envelope.Payload, messageType, EventSerializer.Options)!;
        foreach (var subscription in catalog.SubscriptionsFor(envelope.MessageType))
        {
            await using var scope = scopeFactory.CreateAsyncScope();
            var handler = scope.ServiceProvider.GetRequiredService(subscription.HandlerType);
            await subscription.InvokeAsync(handler, message, cancellationToken);
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Dropping message {MessageId} of unknown type {MessageType}")]
    private static partial void LogUnknown(ILogger logger, string messageType, Guid messageId);
}

/// <summary>Synchronous in-process transport (tests, single-process dev).</summary>
public sealed class InMemoryMessageBus(MessageDispatcher dispatcher) : IMessageBus
{
    public async Task<IReadOnlyList<Exception?>> PublishAsync(IReadOnlyList<MessageEnvelope> envelopes, CancellationToken cancellationToken)
    {
        var results = new Exception?[envelopes.Count];
        for (var i = 0; i < envelopes.Count; i++)
        {
            try
            {
                await dispatcher.DispatchAsync(envelopes[i], cancellationToken);
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                results[i] = exception;
            }
        }

        return results;
    }
}
