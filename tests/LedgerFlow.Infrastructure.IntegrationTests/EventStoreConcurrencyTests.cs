using LedgerFlow.Application.Abstractions;
using LedgerFlow.Application.Abstractions.Messaging;
using LedgerFlow.Application.Features.Ledger;
using LedgerFlow.Application.Features.Payments;
using LedgerFlow.Application.Messaging;
using LedgerFlow.Domain.Accounts;
using LedgerFlow.Infrastructure.Messaging;
using LedgerFlow.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace LedgerFlow.Infrastructure.IntegrationTests;

[Collection(SqlServerCollection.Name)]
public sealed class EventStoreConcurrencyTests(SqlServerFixture fixture)
{
    [Fact]
    public async Task Concurrent_appends_at_the_same_expected_version_have_exactly_one_winner()
    {
        Assert.SkipUnless(fixture.Available, fixture.SkipReason);
        var accountId = await fixture.OpenFundedAccountAsync(100m);

        // 20 writers read version N, then all try to append N+1 at once.
        var scopes = Enumerable.Range(0, 20).Select(_ => fixture.Services.CreateAsyncScope()).ToList();
        var sessions = new List<ILedgerSession>();
        foreach (var scope in scopes)
        {
            var session = fixture.Session(scope.ServiceProvider);
            var account = await session.LoadAsync<Account>(accountId, CancellationToken.None);
            account!.Freeze("race");
            session.Track(account);
            sessions.Add(session);
        }

        var outcomes = await Task.WhenAll(sessions.Select(async session =>
        {
            try
            {
                await session.CommitAsync(CancellationToken.None);
                return true;
            }
            catch (ConcurrencyConflictException)
            {
                return false;
            }
        }));

        foreach (var scope in scopes)
        {
            await scope.DisposeAsync();
        }

        Assert.Equal(1, outcomes.Count(won => won));
        var events = await fixture.InScopeAsync(sp => sp.GetRequiredService<LedgerDbContext>().Events
            .CountAsync(e => e.StreamId == Account.StreamName(accountId) && e.EventType == nameof(AccountFrozen)));
        Assert.Equal(1, events);
    }

    [Fact]
    public async Task A_conflict_on_one_stream_rolls_back_every_stream_and_message_of_the_commit()
    {
        Assert.SkipUnless(fixture.Available, fixture.SkipReason);
        var first = await fixture.OpenFundedAccountAsync(50m);
        var second = await fixture.OpenFundedAccountAsync(50m);

        await using var loser = fixture.Services.CreateAsyncScope();
        var session = fixture.Session(loser.ServiceProvider);
        var a = (await session.LoadAsync<Account>(first, CancellationToken.None))!;
        var b = (await session.LoadAsync<Account>(second, CancellationToken.None))!;
        a.Freeze("multi-stream");
        b.Freeze("multi-stream");
        session.Track(a);
        session.Track(b);
        session.Publish(new PaymentRejected(Guid.NewGuid(), first, "Test", "must not be published"));

        // Someone else moves the second stream first.
        await fixture.InScopeAsync(async sp =>
        {
            var other = fixture.Session(sp);
            var account = (await other.LoadAsync<Account>(second, CancellationToken.None))!;
            account.Freeze("winner");
            other.Track(account);
            await other.CommitAsync(CancellationToken.None);
            return true;
        });

        await Assert.ThrowsAsync<ConcurrencyConflictException>(() => session.CommitAsync(CancellationToken.None));

        Assert.Equal(AccountStatus.Active, (await fixture.AccountAsync(first)).Status);
        var leakedMessages = await fixture.InScopeAsync(sp => sp.GetRequiredService<LedgerDbContext>().Outbox
            .CountAsync(m => m.MessageType == nameof(PaymentRejected) && m.Payload.Contains("must not be published")));
        Assert.Equal(0, leakedMessages);
    }

    [Fact]
    public async Task Parallel_reservations_against_one_account_never_overdraw_it()
    {
        Assert.SkipUnless(fixture.Available, fixture.SkipReason);
        var source = await fixture.OpenFundedAccountAsync(100m);
        var destination = await fixture.OpenFundedAccountAsync(0m);

        var payments = new List<Guid>();
        for (var i = 0; i < 30; i++)
        {
            payments.Add((await fixture.SendAsync(new InitiatePaymentCommand("tests", $"overdraw-{source:N}-{i}", source, destination, 10m, "TRY", null))).PaymentId);
        }

        // No partitioning here on purpose: the optimistic concurrency of the event store alone must keep the invariant.
        var outcomes = await Task.WhenAll(payments.Select(id => fixture.SendAsync(new ReservePaymentFundsCommand(id))));

        Assert.Equal(10, outcomes.Count(o => o == SagaStepOutcome.Advanced));
        Assert.Equal(20, outcomes.Count(o => o == SagaStepOutcome.Failed));
        var account = await fixture.AccountAsync(source);
        Assert.Equal((100m, 100m, 0m), (account.Balance, account.Held, account.Available));
    }

    [Fact]
    public async Task Concurrent_retries_of_one_request_create_a_single_payment()
    {
        Assert.SkipUnless(fixture.Available, fixture.SkipReason);
        var source = await fixture.OpenFundedAccountAsync(100m);
        var destination = await fixture.OpenFundedAccountAsync(0m);
        var key = $"retry-{Guid.NewGuid():N}";

        var responses = await Task.WhenAll(Enumerable.Range(0, 12).Select(_ =>
            fixture.SendAsync(new InitiatePaymentCommand("tests", key, source, destination, 25m, "TRY", "same request"))));

        Assert.Single(responses.Select(r => r.PaymentId).Distinct());
        Assert.Equal(11, responses.Count(r => r.Replayed));
        var streams = await fixture.InScopeAsync(sp => sp.GetRequiredService<LedgerDbContext>().Events
            .CountAsync(e => e.StreamId == $"payment-{responses[0].PaymentId:N}"));
        Assert.Equal(1, streams);
    }

    [Fact]
    public async Task Concurrent_outbox_relays_publish_every_row_exactly_once()
    {
        Assert.SkipUnless(fixture.Available, fixture.SkipReason);

        // Drain whatever earlier tests left, then enqueue a known batch.
        var drain = new CountingBus();
        while (await NewRelay(drain).RelayBatchAsync(CancellationToken.None) > 0)
        {
        }

        var expected = new HashSet<Guid>();
        for (var batch = 0; batch < 10; batch++)
        {
            await fixture.InScopeAsync(async sp =>
            {
                var session = fixture.Session(sp);
                for (var i = 0; i < 200; i++)
                {
                    var paymentId = Guid.NewGuid();
                    expected.Add(paymentId);
                    session.Publish(new PaymentRejected(paymentId, Guid.NewGuid(), "Load", "relay test"));
                }

                await session.CommitAsync(CancellationToken.None);
                return true;
            });
        }

        var bus = new CountingBus();
        await Task.WhenAll(Enumerable.Range(0, 3).Select(async _ =>
        {
            var relay = NewRelay(bus);
            while (await relay.RelayBatchAsync(CancellationToken.None) > 0)
            {
            }
        }));

        var published = bus.Envelopes.Where(e => e.Payload.Contains("relay test", StringComparison.Ordinal)).ToList();
        Assert.Equal(2_000, published.Count);
        Assert.Equal(2_000, published.Select(e => e.MessageId).Distinct().Count());
    }

    [Fact]
    public async Task Reconciliation_is_clean_after_concurrent_saga_traffic()
    {
        Assert.SkipUnless(fixture.Available, fixture.SkipReason);
        var accounts = new List<Guid>();
        for (var i = 0; i < 6; i++)
        {
            accounts.Add(await fixture.OpenFundedAccountAsync(500m));
        }

        var random = new Random(7);
        var payments = new List<Guid>();
        for (var i = 0; i < 60; i++)
        {
            var from = accounts[random.Next(accounts.Count)];
            var to = accounts.First(a => a != from);
            var payment = await fixture.SendAsync(new InitiatePaymentCommand("tests", Guid.NewGuid().ToString(), from, to, random.Next(1, 200), "TRY", null));
            payments.Add(payment.PaymentId);
        }

        await Task.WhenAll(payments.Select(id => fixture.SendAsync(new ReservePaymentFundsCommand(id))));
        await Task.WhenAll(payments.Select(id => fixture.SendAsync(new AssessPaymentRiskCommand(id))));
        await Task.WhenAll(payments.Select(id => fixture.SendAsync(new SettlePaymentCommand(id))));

        var report = await fixture.InScopeAsync(async sp => (await sp.GetRequiredService<ISender>().Send(new ReconcileLedgerQuery())).Value);
        Assert.True(report.IsClean, string.Join("; ", report.Issues.Select(i => $"{i.Kind}:{i.Detail}")));
    }

    private OutboxRelay NewRelay(IMessageBus bus) => new(
        fixture.Services.GetRequiredService<IServiceScopeFactory>(),
        bus,
        fixture.Services.GetRequiredService<IOptions<MessagingOptions>>(),
        TimeProvider.System,
        NullLogger<OutboxRelay>.Instance);

    private sealed class CountingBus : IMessageBus
    {
        private readonly System.Collections.Concurrent.ConcurrentBag<MessageEnvelope> _envelopes = [];

        public IReadOnlyCollection<MessageEnvelope> Envelopes => _envelopes;

        public Task<IReadOnlyList<Exception?>> PublishAsync(IReadOnlyList<MessageEnvelope> envelopes, CancellationToken cancellationToken)
        {
            foreach (var envelope in envelopes)
            {
                _envelopes.Add(envelope);
            }

            return Task.FromResult<IReadOnlyList<Exception?>>(new Exception?[envelopes.Count]);
        }
    }
}
