using LedgerFlow.Application.Messaging;
using LedgerFlow.Application.ReadModels;
using LedgerFlow.Domain.Common;

namespace LedgerFlow.Application.Abstractions;

/// <summary>
/// Unit of work over the event store. Everything tracked in one session — events of several streams, their
/// read-model projections and outgoing messages — is committed atomically, or not at all.
/// </summary>
public interface ILedgerSession
{
    Task<T?> LoadAsync<T>(Guid id, CancellationToken cancellationToken)
        where T : EventSourcedAggregate, IEventSourced<T>;

    /// <summary>Temporal query: state as it was at <paramref name="asOfUtc"/>, rebuilt from the closest earlier snapshot.</summary>
    Task<T?> LoadAsOfAsync<T>(Guid id, DateTime asOfUtc, CancellationToken cancellationToken)
        where T : EventSourcedAggregate, IEventSourced<T>;

    void Track(EventSourcedAggregate aggregate);

    /// <summary>Adds a message to the transactional outbox of this unit of work.</summary>
    void Publish(IIntegrationMessage message);

    /// <exception cref="ConcurrencyConflictException">A stream moved past the expected version.</exception>
    Task CommitAsync(CancellationToken cancellationToken);

    /// <summary>Forgets tracked aggregates and pending messages (used before a retry).</summary>
    void Reset();
}

public sealed class ConcurrencyConflictException(string message, Exception? innerException = null)
    : Exception(message, innerException);

public interface ILedgerReadStore
{
    IQueryable<AccountView> Accounts { get; }

    IQueryable<LedgerEntryView> LedgerEntries { get; }

    IQueryable<PaymentView> Payments { get; }
}

public sealed record IntegrityReport(string StreamId, long EventCount, bool IsIntact, long? FirstBrokenVersion, string? Detail);

public interface IEventStoreAudit
{
    Task<IntegrityReport> VerifyStreamAsync(string streamId, CancellationToken cancellationToken);

    Task<long> CountEventsAsync(CancellationToken cancellationToken);

    Task<long> CountPendingOutboxAsync(CancellationToken cancellationToken);
}

public sealed record RiskRequest(Guid PaymentId, Guid FromAccountId, Guid ToAccountId, long AmountMinor, string Currency);

public sealed record RiskDecision(bool Approved, int Score, string? Reason);

/// <summary>External fraud/risk engine. Implementations throw when the engine is unavailable so the step is retried.</summary>
public interface IRiskAssessor
{
    Task<RiskDecision> AssessAsync(RiskRequest request, CancellationToken cancellationToken);
}
