using LedgerFlow.Application.Abstractions;
using LedgerFlow.Application.Abstractions.Messaging;
using LedgerFlow.Application.Features.Accounts;
using LedgerFlow.Application.Features.Ledger;
using LedgerFlow.Domain.Accounts;
using LedgerFlow.Domain.Transactions;
using LedgerFlow.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace LedgerFlow.Infrastructure.IntegrationTests;

[Collection(SqlServerCollection.Name)]
public sealed class AuditAndTemporalTests(SqlServerFixture fixture)
{
    [Fact]
    public async Task Editing_a_stored_event_is_detected_by_the_hash_chain()
    {
        Assert.SkipUnless(fixture.Available, fixture.SkipReason);
        var accountId = await fixture.OpenFundedAccountAsync(100m);
        await Deposit(accountId, 50m);
        var streamId = Account.StreamName(accountId);

        Assert.True((await Verify(streamId)).IsIntact);

        // An insider "corrects" the first deposit directly in the database.
        await fixture.InScopeAsync(sp => sp.GetRequiredService<LedgerDbContext>().Events
            .Where(e => e.StreamId == streamId && e.StreamVersion == 1)
            .ExecuteUpdateAsync(s => s.SetProperty(e => e.Payload, e => e.Payload.Replace("10000", "99900"))));

        var report = await Verify(streamId);
        Assert.False(report.IsIntact);
        Assert.Equal(1, report.FirstBrokenVersion);
    }

    [Fact]
    public async Task Balance_as_of_a_past_instant_is_rebuilt_from_snapshots_and_events()
    {
        Assert.SkipUnless(fixture.Available, fixture.SkipReason);
        var accountId = await fixture.OpenFundedAccountAsync(0m);
        DateTime? checkpoint = null;

        for (var day = 1; day <= 25; day++)
        {
            fixture.Clock.Advance(TimeSpan.FromDays(1));
            await Deposit(accountId, 10m);
            if (day == 12)
            {
                checkpoint = fixture.Clock.GetUtcNow().UtcDateTime.AddHours(1);
            }
        }

        var historic = await fixture.InScopeAsync(async sp =>
            (await sp.GetRequiredService<ISender>().Send(new GetBalanceAtQuery(accountId, checkpoint!.Value))).Value);
        var current = await fixture.AccountAsync(accountId);
        var snapshots = await fixture.InScopeAsync(sp => sp.GetRequiredService<LedgerDbContext>().Snapshots
            .CountAsync(s => s.StreamId == Account.StreamName(accountId)));

        Assert.Equal(120m, historic.Balance);
        Assert.Equal(12, historic.Version);
        Assert.Equal(250m, current.Balance);
        Assert.Equal(2, snapshots); // SnapshotEvery = 10 → after versions 9 and 19
    }

    [Fact]
    public async Task Loading_from_a_snapshot_yields_the_same_state_as_a_full_replay()
    {
        Assert.SkipUnless(fixture.Available, fixture.SkipReason);
        var accountId = await fixture.OpenFundedAccountAsync(0m);
        for (var i = 0; i < 23; i++)
        {
            await Deposit(accountId, 1.11m);
        }

        var viaSnapshot = await fixture.InScopeAsync(sp => fixture.Session(sp).LoadAsync<Account>(accountId, CancellationToken.None));
        await fixture.InScopeAsync(sp => sp.GetRequiredService<LedgerDbContext>().Snapshots
            .Where(s => s.StreamId == Account.StreamName(accountId)).ExecuteDeleteAsync());
        var viaReplay = await fixture.InScopeAsync(sp => fixture.Session(sp).LoadAsync<Account>(accountId, CancellationToken.None));

        Assert.Equal((viaReplay!.BalanceMinor, viaReplay.Version, viaReplay.LastHash), (viaSnapshot!.BalanceMinor, viaSnapshot.Version, viaSnapshot.LastHash));
        Assert.Equal(2_553, viaSnapshot.BalanceMinor);
    }

    private Task<TransactionResponse> Deposit(Guid accountId, decimal amount) =>
        fixture.SendAsync(new MoveFundsCommand(TransactionKind.Deposit, "tests", Guid.NewGuid().ToString(), accountId, amount, "TRY", null));

    private Task<IntegrityReport> Verify(string streamId) => fixture.InScopeAsync(async sp =>
        (await sp.GetRequiredService<ISender>().Send(new VerifyStreamIntegrityQuery(streamId))).Value);
}
