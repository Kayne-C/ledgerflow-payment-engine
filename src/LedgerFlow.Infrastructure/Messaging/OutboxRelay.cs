using LedgerFlow.Application.Diagnostics;
using LedgerFlow.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LedgerFlow.Infrastructure.Messaging;

public interface IOutboxRelay
{
    /// <returns>Number of messages claimed in this batch.</returns>
    Task<int> RelayBatchAsync(CancellationToken cancellationToken);
}

/// <summary>
/// Relays committed outbox rows to the broker. Rows are first <b>leased</b> with a conditional UPDATE, so several
/// relay replicas split the work instead of publishing the same rows twice; an expired lease (crashed relay) makes
/// rows claimable again. Delivery stays at-least-once — consumers are idempotent.
/// </summary>
public sealed partial class OutboxRelay(
    IServiceScopeFactory scopeFactory,
    IMessageBus bus,
    IOptions<MessagingOptions> options,
    TimeProvider clock,
    ILogger<OutboxRelay> logger) : BackgroundService, IOutboxRelay
{
    private readonly OutboxRelayOptions _options = options.Value.Outbox;

    public async Task<int> RelayBatchAsync(CancellationToken cancellationToken)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<LedgerDbContext>();
        var now = clock.GetUtcNow().UtcDateTime;
        var leaseId = Guid.NewGuid();

        var window = await db.Outbox.AsNoTracking()
            .Where(m => m.ProcessedOnUtc == null && m.Attempts < _options.MaxAttempts && (m.LeaseExpiresUtc == null || m.LeaseExpiresUtc < now))
            .OrderBy(m => m.Id)
            .Take(_options.BatchSize)
            .Select(m => m.Id)
            .ToListAsync(cancellationToken);

        if (window.Count == 0)
        {
            return 0;
        }

        // Claim by id range (portable across SQL Server, Oracle and SQLite); the predicate is re-checked atomically.
        long first = window[0], last = window[^1];
        var claimed = await db.Outbox
            .Where(m => m.Id >= first && m.Id <= last && m.ProcessedOnUtc == null && m.Attempts < _options.MaxAttempts
                && (m.LeaseExpiresUtc == null || m.LeaseExpiresUtc < now))
            .ExecuteUpdateAsync(s => s.SetProperty(m => m.LeaseId, leaseId).SetProperty(m => m.LeaseExpiresUtc, now + _options.LeaseDuration), cancellationToken);

        if (claimed == 0)
        {
            return 0;
        }

        var batch = await db.Outbox.Where(m => m.LeaseId == leaseId).OrderBy(m => m.Id).ToListAsync(cancellationToken);
        var results = await bus.PublishAsync(
            batch.Select(m => new MessageEnvelope(m.MessageId, m.Topic, m.PartitionKey, m.MessageType, m.Payload, m.TraceParent)).ToList(),
            cancellationToken);

        var publishedAt = clock.GetUtcNow().UtcDateTime;
        for (var i = 0; i < batch.Count; i++)
        {
            if (results[i] is { } error)
            {
                batch[i].Attempts++;

                // Exponential backoff (1 s, 2 s, 4 s … capped at 5 min): a broker outage is not hammered, and the
                // row stays leased until it is due again.
                batch[i].LeaseExpiresUtc = publishedAt + RetryDelay(batch[i].Attempts);
                batch[i].LastError = error.Message.Length > OutboxRecord.ErrorMaxLength ? error.Message[..OutboxRecord.ErrorMaxLength] : error.Message;
                LogPublishFailed(logger, error, batch[i].MessageId, batch[i].MessageType, batch[i].Attempts);
            }
            else
            {
                batch[i].ProcessedOnUtc = publishedAt;
            }
        }

        await db.SaveChangesAsync(cancellationToken);
        LedgerTelemetry.OutboxPublished.Add(results.Count(r => r is null));
        return batch.Count;
    }

    internal static TimeSpan RetryDelay(int attempts) =>
        TimeSpan.FromSeconds(Math.Min(300, Math.Pow(2, Math.Min(attempts - 1, 9))));

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Enabled)
        {
            return;
        }

        using var timer = new PeriodicTimer(_options.PollingInterval, clock);
        do
        {
            try
            {
                while (await RelayBatchAsync(stoppingToken) > 0)
                {
                }
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                LogRelayFailed(logger, exception);
            }
        }
        while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Publishing outbox message {MessageId} ({Type}) failed, attempt {Attempt}")]
    private static partial void LogPublishFailed(ILogger logger, Exception exception, Guid messageId, string type, int attempt);

    [LoggerMessage(Level = LogLevel.Error, Message = "Outbox relay iteration failed")]
    private static partial void LogRelayFailed(ILogger logger, Exception exception);
}
