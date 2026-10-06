namespace LedgerFlow.Domain.Common;

/// <summary>A fact that happened. Persisted as the source of truth; state is derived by replaying events.</summary>
public interface IDomainEvent;

/// <summary>Static factory contract so repositories can create and name streams without reflection.</summary>
public interface IEventSourced<out TSelf>
    where TSelf : EventSourcedAggregate
{
    static abstract string StreamName(Guid id);

    static abstract TSelf CreateEmpty();
}

/// <summary>
/// Event-sourced aggregate. <see cref="Version"/> is the version of the last persisted event (-1 = no stream);
/// it is the expected version for the next append, which makes every write an optimistic-concurrency check.
/// </summary>
public abstract class EventSourcedAggregate
{
    public const long NoStream = -1;

    private readonly List<IDomainEvent> _uncommittedEvents = [];

    public Guid Id { get; protected set; }

    public long Version { get; private set; } = NoStream;

    /// <summary>Hash of the last persisted event; the next event chains onto it (tamper evidence).</summary>
    public string? LastHash { get; private set; }

    public abstract string StreamId { get; }

    public IReadOnlyList<IDomainEvent> UncommittedEvents => _uncommittedEvents;

    public void Replay(IDomainEvent domainEvent, long version, string hash)
    {
        Apply(domainEvent);
        Version = version;
        LastHash = hash;
    }

    public void MarkCommitted(long version, string lastHash)
    {
        _uncommittedEvents.Clear();
        Version = version;
        LastHash = lastHash;
    }

    protected void RestoreVersion(long version, string? lastHash)
    {
        Version = version;
        LastHash = lastHash;
    }

    protected void Raise(IDomainEvent domainEvent)
    {
        Apply(domainEvent);
        _uncommittedEvents.Add(domainEvent);
    }

    protected abstract void Apply(IDomainEvent domainEvent);
}
