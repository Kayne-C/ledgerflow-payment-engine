using System.Reflection;

namespace LedgerFlow.Application.Messaging;

/// <summary>Message leaving the process through the transactional outbox.</summary>
public interface IIntegrationMessage
{
    /// <summary>
    /// Broker partition key. Messages with the same key are delivered in order to a single consumer, which is how
    /// all debits of one account are serialized (single writer) while different accounts proceed in parallel.
    /// </summary>
    string PartitionKey { get; }
}

/// <summary>Internal saga command (commands topic).</summary>
public interface ISagaCommand : IIntegrationMessage;

/// <summary>Public lifecycle event for downstream systems (events topic).</summary>
public interface IPublicEvent : IIntegrationMessage;

public sealed record ReservePaymentFunds(Guid PaymentId, Guid FromAccountId) : ISagaCommand
{
    public string PartitionKey => FromAccountId.ToString("N");
}

public sealed record AssessPaymentRisk(Guid PaymentId) : ISagaCommand
{
    public string PartitionKey => PaymentId.ToString("N");
}

public sealed record SettlePayment(Guid PaymentId, Guid FromAccountId) : ISagaCommand
{
    public string PartitionKey => FromAccountId.ToString("N");
}

public sealed record ReleasePaymentFunds(Guid PaymentId, Guid FromAccountId, string ReasonCode, string Reason) : ISagaCommand
{
    public string PartitionKey => FromAccountId.ToString("N");
}

public sealed record PaymentSucceeded(Guid PaymentId, Guid TransactionId, Guid FromAccountId, Guid ToAccountId, long AmountMinor, string Currency)
    : IPublicEvent
{
    public string PartitionKey => PaymentId.ToString("N");
}

public sealed record PaymentRejected(Guid PaymentId, Guid FromAccountId, string ReasonCode, string Reason) : IPublicEvent
{
    public string PartitionKey => PaymentId.ToString("N");
}

/// <summary>Consumer of one message type. Delivery is at-least-once; implementations must be idempotent.</summary>
public interface IMessageHandler<in TMessage>
    where TMessage : IIntegrationMessage
{
    Task HandleAsync(TMessage message, CancellationToken cancellationToken);
}

public sealed class MessageSubscription
{
    private static readonly MethodInfo InvokeMethod =
        typeof(MessageSubscription).GetMethod(nameof(InvokeTyped), BindingFlags.NonPublic | BindingFlags.Static)!;

    private readonly Func<object, IIntegrationMessage, CancellationToken, Task> _invoker;

    public MessageSubscription(Type messageType, Type handlerType)
    {
        MessageType = messageType;
        HandlerType = handlerType;
        _invoker = InvokeMethod.MakeGenericMethod(messageType)
            .CreateDelegate<Func<object, IIntegrationMessage, CancellationToken, Task>>();
    }

    public Type MessageType { get; }

    public Type HandlerType { get; }

    public string MessageName => MessageType.Name;

    public Task InvokeAsync(object handler, IIntegrationMessage message, CancellationToken cancellationToken) =>
        _invoker(handler, message, cancellationToken);

    private static Task InvokeTyped<TMessage>(object handler, IIntegrationMessage message, CancellationToken cancellationToken)
        where TMessage : IIntegrationMessage =>
        ((IMessageHandler<TMessage>)handler).HandleAsync((TMessage)message, cancellationToken);
}

/// <summary>Known message types by name — the only types the consumer will ever deserialize.</summary>
public sealed class MessageCatalog(IEnumerable<MessageSubscription> subscriptions)
{
    private static readonly Dictionary<string, Type> Types = typeof(IIntegrationMessage).Assembly.GetTypes()
        .Where(t => t is { IsClass: true, IsAbstract: false } && typeof(IIntegrationMessage).IsAssignableFrom(t))
        .ToDictionary(t => t.Name);

    private readonly ILookup<string, MessageSubscription> _subscriptions = subscriptions.ToLookup(s => s.MessageName);

    public IEnumerable<MessageSubscription> SubscriptionsFor(string messageName) => _subscriptions[messageName];

    public static Type? Resolve(string messageName) => Types.GetValueOrDefault(messageName);
}
