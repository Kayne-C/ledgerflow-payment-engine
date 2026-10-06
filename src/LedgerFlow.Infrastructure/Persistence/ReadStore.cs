using LedgerFlow.Application.Abstractions;
using LedgerFlow.Application.ReadModels;
using LedgerFlow.Domain.Common;
using Microsoft.EntityFrameworkCore;

namespace LedgerFlow.Infrastructure.Persistence;

internal sealed class ReadStore(LedgerDbContext db) : ILedgerReadStore
{
    public IQueryable<AccountView> Accounts => db.AccountViews.AsNoTracking();

    public IQueryable<LedgerEntryView> LedgerEntries => db.LedgerEntries.AsNoTracking();

    public IQueryable<PaymentView> Payments => db.PaymentViews.AsNoTracking();
}

internal sealed class EventStoreAudit(LedgerDbContext db) : IEventStoreAudit
{
    public async Task<IntegrityReport> VerifyStreamAsync(string streamId, CancellationToken cancellationToken)
    {
        string? previous = null;
        long expectedVersion = 0, count = 0;

        // Streamed, so verifying a long stream does not load it into memory at once.
        await foreach (var record in db.Events.AsNoTracking()
                           .Where(e => e.StreamId == streamId)
                           .OrderBy(e => e.StreamVersion)
                           .Select(e => new { e.StreamVersion, e.EventType, e.Payload, e.PreviousHash, e.Hash })
                           .AsAsyncEnumerable()
                           .WithCancellation(cancellationToken))
        {
            count++;
            if (record.StreamVersion != expectedVersion)
            {
                return new IntegrityReport(streamId, count, false, expectedVersion, $"Version {expectedVersion} is missing.");
            }

            var recomputed = HashChain.Compute(previous, streamId, record.StreamVersion, record.EventType, record.Payload);
            if (record.PreviousHash?.Trim() != previous || record.Hash.Trim() != recomputed)
            {
                return new IntegrityReport(streamId, count, false, record.StreamVersion, "Hash chain broken: the event or its predecessor was altered.");
            }

            previous = recomputed;
            expectedVersion++;
        }

        return new IntegrityReport(streamId, count, true, null, null);
    }

    public Task<long> CountEventsAsync(CancellationToken cancellationToken) => db.Events.LongCountAsync(cancellationToken);

    public Task<long> CountPendingOutboxAsync(CancellationToken cancellationToken) =>
        db.Outbox.LongCountAsync(m => m.ProcessedOnUtc == null, cancellationToken);
}
