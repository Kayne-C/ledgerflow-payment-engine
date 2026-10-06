namespace LedgerFlow.Infrastructure.Persistence;

/// <summary>Append-only event log row. (StreamId, StreamVersion) is unique: that index is the optimistic-concurrency check.</summary>
public sealed class EventRecord
{
    public const int StreamIdMaxLength = 100;
    public const int TypeMaxLength = 128;
    public const int HashLength = 64;
    public const int PayloadMaxLength = 1_000_000;

    /// <summary>Total order across all streams (identity column).</summary>
    public long GlobalPosition { get; set; }

    public string StreamId { get; set; } = null!;

    public long StreamVersion { get; set; }

    public Guid EventId { get; set; }

    public string EventType { get; set; } = null!;

    public string Payload { get; set; } = null!;

    /// <summary>W3C traceparent of the command that produced the event.</summary>
    public string? TraceParent { get; set; }

    public DateTime RecordedAtUtc { get; set; }

    public string? PreviousHash { get; set; }

    public string Hash { get; set; } = null!;
}

/// <summary>Point-in-time aggregate state. Several are kept per stream so temporal queries can start close to the requested time.</summary>
public sealed class SnapshotRecord
{
    public string StreamId { get; set; } = null!;

    public long Version { get; set; }

    public string Payload { get; set; } = null!;

    public string? LastHash { get; set; }

    public DateTime LastEventRecordedAtUtc { get; set; }
}

/// <summary>Transactional outbox row with a lease, so several relays can share the table without publishing the same row twice.</summary>
public sealed class OutboxRecord
{
    public const int TopicMaxLength = 128;
    public const int ErrorMaxLength = 2000;

    public long Id { get; set; }

    public string Topic { get; set; } = null!;

    public string PartitionKey { get; set; } = null!;

    public string MessageType { get; set; } = null!;

    public Guid MessageId { get; set; }

    public string Payload { get; set; } = null!;

    public string? TraceParent { get; set; }

    public DateTime OccurredOnUtc { get; set; }

    public Guid? LeaseId { get; set; }

    public DateTime? LeaseExpiresUtc { get; set; }

    public DateTime? ProcessedOnUtc { get; set; }

    public int Attempts { get; set; }

    public string? LastError { get; set; }
}
