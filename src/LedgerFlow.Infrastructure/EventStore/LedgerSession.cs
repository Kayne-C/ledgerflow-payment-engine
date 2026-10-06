using System.Diagnostics;
using System.Text.Json;
using LedgerFlow.Application.Abstractions;
using LedgerFlow.Application.Messaging;
using LedgerFlow.Application.ReadModels;
using LedgerFlow.Domain.Accounts;
using LedgerFlow.Domain.Common;
using LedgerFlow.Domain.Payments;
using LedgerFlow.Domain.Transactions;
using LedgerFlow.Infrastructure.Messaging;
using LedgerFlow.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;

namespace LedgerFlow.Infrastructure.EventStore;

public sealed class EventStoreOptions
{
    public const string Section = "EventStore";

    /// <summary>An account snapshot is written every N events.</summary>
    public int SnapshotEvery { get; set; } = 25;

    /// <summary>Hot accounts kept in the per-process cache (0 disables it).</summary>
    public int AccountCacheSize { get; set; } = 50_000;
}

/// <summary>
/// Per-process cache of committed account state. Because events are immutable and append-only, a cached state at
/// version V is always a valid prefix: loading only reads events after V (usually none). Partitioning saga commands
/// by account sends an account's traffic to the same consumer, so hit rates are high.
/// </summary>
public sealed class AccountStateCache(IOptions<EventStoreOptions> options) : IDisposable
{
    private readonly MemoryCache? _cache = options.Value.AccountCacheSize > 0
        ? new MemoryCache(new MemoryCacheOptions { SizeLimit = options.Value.AccountCacheSize })
        : null;

    public bool TryGet(string streamId, out (AccountSnapshot State, long Version, string? Hash) entry)
    {
        if (_cache is not null && _cache.TryGetValue(streamId, out (AccountSnapshot, long, string?) cached))
        {
            entry = cached;
            return true;
        }

        entry = default;
        return false;
    }

    public void Set(Account account)
    {
        if (_cache is null || account.Version < 0)
        {
            return;
        }

        var key = account.StreamId;
        var value = (account.ToSnapshot(), account.Version, account.LastHash);

        // Never replace a newer state with an older one (two consumers may finish out of order).
        if (_cache.TryGetValue(key, out (AccountSnapshot, long Version, string?) existing) && existing.Version >= account.Version)
        {
            return;
        }

        _cache.Set(key, value, new MemoryCacheEntryOptions { Size = 1, SlidingExpiration = TimeSpan.FromMinutes(10) });
    }

    public void Dispose() => _cache?.Dispose();
}

/// <summary>
/// Relational event store unit of work. One <see cref="CommitAsync"/> = one database transaction containing:
/// new events of every tracked stream (hash-chained, versioned), their read-model projections, snapshots and
/// outbox messages. A concurrent writer on any of the streams makes the whole commit fail and retry.
/// </summary>
internal sealed class LedgerSession(
    LedgerDbContext db,
    TimeProvider clock,
    IOptions<EventStoreOptions> options,
    IOptions<MessagingOptions> messaging,
    AccountStateCache accountCache) : ILedgerSession
{
    private readonly Dictionary<string, EventSourcedAggregate> _identityMap = new(StringComparer.Ordinal);
    private readonly Dictionary<string, EventSourcedAggregate> _tracked = new(StringComparer.Ordinal);
    private readonly List<IIntegrationMessage> _outbox = [];

    public async Task<T?> LoadAsync<T>(Guid id, CancellationToken cancellationToken)
        where T : EventSourcedAggregate, IEventSourced<T>
    {
        var streamId = T.StreamName(id);
        if (_identityMap.TryGetValue(streamId, out var cached))
        {
            return (T)cached;
        }

        var aggregate = await RehydrateAsync<T>(streamId, asOfUtc: null, cancellationToken);
        if (aggregate is not null)
        {
            _identityMap[streamId] = aggregate;
        }

        return aggregate;
    }

    public Task<T?> LoadAsOfAsync<T>(Guid id, DateTime asOfUtc, CancellationToken cancellationToken)
        where T : EventSourcedAggregate, IEventSourced<T> =>
        RehydrateAsync<T>(T.StreamName(id), asOfUtc, cancellationToken);

    public void Track(EventSourcedAggregate aggregate)
    {
        _tracked[aggregate.StreamId] = aggregate;
        _identityMap[aggregate.StreamId] = aggregate;
    }

    public void Publish(IIntegrationMessage message) => _outbox.Add(message);

    public async Task CommitAsync(CancellationToken cancellationToken)
    {
        var utcNow = clock.GetUtcNow().UtcDateTime;
        var traceParent = Activity.Current?.Id;
        var committed = new List<(EventSourcedAggregate Aggregate, long Version, string Hash)>();

        // Stable stream order => stable lock order across concurrent transactions (fewer deadlocks).
        foreach (var aggregate in _tracked.Values.Where(a => a.UncommittedEvents.Count > 0).OrderBy(a => a.StreamId, StringComparer.Ordinal))
        {
            var version = aggregate.Version;
            var hash = aggregate.LastHash;
            foreach (var domainEvent in aggregate.UncommittedEvents)
            {
                version++;
                var type = EventSerializer.TypeName(domainEvent);
                var payload = EventSerializer.Serialize(domainEvent);
                var previous = hash;
                hash = HashChain.Compute(previous, aggregate.StreamId, version, type, payload);

                db.Events.Add(new EventRecord
                {
                    StreamId = aggregate.StreamId,
                    StreamVersion = version,
                    EventId = Guid.CreateVersion7(),
                    EventType = type,
                    Payload = payload,
                    TraceParent = traceParent,
                    RecordedAtUtc = utcNow,
                    PreviousHash = previous,
                    Hash = hash!,
                });
            }

            Project(aggregate, utcNow);
            Snapshot(aggregate, version, hash!, utcNow);
            committed.Add((aggregate, version, hash!));
        }

        foreach (var message in _outbox)
        {
            db.Outbox.Add(new OutboxRecord
            {
                Topic = message is ISagaCommand ? messaging.Value.Kafka.CommandsTopic : messaging.Value.Kafka.EventsTopic,
                PartitionKey = message.PartitionKey,
                MessageType = message.GetType().Name,
                MessageId = Guid.CreateVersion7(),
                Payload = JsonSerializer.Serialize(message, message.GetType(), EventSerializer.Options),
                TraceParent = traceParent,
                OccurredOnUtc = utcNow,
            });
        }

        try
        {
            await db.SaveChangesAsync(cancellationToken);
        }
        catch (DbUpdateException exception) when (LedgerDbContext.IsConcurrencyFailure(exception))
        {
            Reset();
            throw new ConcurrencyConflictException("A stream was modified concurrently; reload and retry.", exception);
        }
        catch
        {
            Reset();
            throw;
        }

        foreach (var (aggregate, version, hash) in committed)
        {
            aggregate.MarkCommitted(version, hash);
            if (aggregate is Account account)
            {
                accountCache.Set(account);
            }
        }

        _tracked.Clear();
        _outbox.Clear();
        db.ChangeTracker.Clear();
    }

    public void Reset()
    {
        _identityMap.Clear();
        _tracked.Clear();
        _outbox.Clear();
        db.ChangeTracker.Clear();
    }

    private async Task<T?> RehydrateAsync<T>(string streamId, DateTime? asOfUtc, CancellationToken cancellationToken)
        where T : EventSourcedAggregate, IEventSourced<T>
    {
        T? aggregate = null;
        var fromVersion = EventSourcedAggregate.NoStream;

        if (typeof(T) == typeof(Account) && asOfUtc is null && accountCache.TryGet(streamId, out var cached))
        {
            aggregate = (T)(EventSourcedAggregate)Account.FromSnapshot(cached.State, cached.Version, cached.Hash);
            fromVersion = cached.Version;
        }
        else if (typeof(T) == typeof(Account))
        {
            var snapshots = db.Snapshots.AsNoTracking().Where(s => s.StreamId == streamId);
            if (asOfUtc is { } asOf)
            {
                snapshots = snapshots.Where(s => s.LastEventRecordedAtUtc <= asOf);
            }

            var snapshot = await snapshots.OrderByDescending(s => s.Version).FirstOrDefaultAsync(cancellationToken);
            if (snapshot is not null)
            {
                var state = JsonSerializer.Deserialize<AccountSnapshot>(snapshot.Payload, EventSerializer.Options)!;
                aggregate = (T)(EventSourcedAggregate)Account.FromSnapshot(state, snapshot.Version, snapshot.LastHash);
                fromVersion = snapshot.Version;
            }
        }

        var events = db.Events.AsNoTracking().Where(e => e.StreamId == streamId && e.StreamVersion > fromVersion);
        if (asOfUtc is { } until)
        {
            events = events.Where(e => e.RecordedAtUtc <= until);
        }

        var records = await events
            .OrderBy(e => e.StreamVersion)
            .Select(e => new { e.StreamVersion, e.EventType, e.Payload, e.Hash })
            .ToListAsync(cancellationToken);

        if (records.Count > 0)
        {
            aggregate ??= T.CreateEmpty();
            foreach (var record in records)
            {
                aggregate.Replay(EventSerializer.Deserialize(record.EventType, record.Payload), record.StreamVersion, record.Hash);
            }
        }

        if (asOfUtc is null && aggregate is Account loaded)
        {
            accountCache.Set(loaded);
        }

        return aggregate;
    }

    /// <summary>Inline projections: read models are rewritten from the aggregate's new state in the same transaction.</summary>
    private void Project(EventSourcedAggregate aggregate, DateTime utcNow)
    {
        var isNew = aggregate.Version == EventSourcedAggregate.NoStream;
        switch (aggregate)
        {
            case Account account:
                var view = new AccountView
                {
                    AccountId = account.Id,
                    Holder = account.Holder,
                    Currency = account.Currency,
                    Kind = account.Kind,
                    Status = account.Status,
                    BalanceMinor = account.BalanceMinor,
                    HeldMinor = account.HeldMinor,
                    Version = account.Version + account.UncommittedEvents.Count,
                    OpenedAtUtc = account.OpenedAtUtc,
                    UpdatedAtUtc = utcNow,
                };
                if (isNew)
                {
                    db.AccountViews.Add(view);
                }
                else
                {
                    db.AccountViews.Update(view);
                }

                foreach (var domainEvent in account.UncommittedEvents)
                {
                    var entry = domainEvent switch
                    {
                        AccountDebited d => (d.TransactionId, PostingDirection.Debit, d.AmountMinor, d.BalanceAfterMinor),
                        AccountCredited c => (c.TransactionId, PostingDirection.Credit, c.AmountMinor, c.BalanceAfterMinor),
                        _ => default((Guid, PostingDirection, long, long)?),
                    };

                    if (entry is { } line)
                    {
                        db.LedgerEntries.Add(new LedgerEntryView
                        {
                            TransactionId = line.Item1,
                            AccountId = account.Id,
                            Direction = line.Item2,
                            AmountMinor = line.Item3,
                            BalanceAfterMinor = line.Item4,
                            Currency = account.Currency,
                            PostedAtUtc = utcNow,
                        });
                    }
                }

                break;

            case Payment payment:
                var paymentView = new PaymentView
                {
                    PaymentId = payment.Id,
                    ClientId = payment.ClientId,
                    FromAccountId = payment.FromAccountId,
                    ToAccountId = payment.ToAccountId,
                    AmountMinor = payment.AmountMinor,
                    Currency = payment.Currency,
                    Reference = payment.Reference,
                    Status = payment.Status,
                    RiskScore = payment.RiskScore,
                    FailureCode = payment.FailureCode,
                    FailureReason = payment.FailureReason is { Length: > 500 } reason ? reason[..500] : payment.FailureReason,
                    InitiatedAtUtc = payment.InitiatedAtUtc,
                    UpdatedAtUtc = utcNow,
                    FinishedAtUtc = payment.FinishedAtUtc,
                };
                if (isNew)
                {
                    db.PaymentViews.Add(paymentView);
                }
                else
                {
                    db.PaymentViews.Update(paymentView);
                }

                break;
        }
    }

    private void Snapshot(EventSourcedAggregate aggregate, long newVersion, string hash, DateTime utcNow)
    {
        var every = options.Value.SnapshotEvery;
        if (aggregate is not Account account || every <= 0 || (newVersion + 1) / every == (account.Version + 1) / every)
        {
            return;
        }

        db.Snapshots.Add(new SnapshotRecord
        {
            StreamId = account.StreamId,
            Version = newVersion,
            Payload = JsonSerializer.Serialize(account.ToSnapshot(), EventSerializer.Options),
            LastHash = hash,
            LastEventRecordedAtUtc = utcNow,
        });
    }
}
