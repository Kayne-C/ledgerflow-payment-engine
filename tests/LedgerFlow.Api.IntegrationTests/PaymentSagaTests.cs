using System.Net;
using System.Net.Http.Json;
using LedgerFlow.Application.Common;
using LedgerFlow.Application.Features.Accounts;
using LedgerFlow.Application.Features.Ledger;
using LedgerFlow.Application.Features.Payments;
using LedgerFlow.Domain.Payments;
using LedgerFlow.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace LedgerFlow.Api.IntegrationTests;

[Collection(ApiCollection.Name)]
public sealed class PaymentSagaTests(LedgerApiFactory factory)
{
    [Fact]
    public async Task A_payment_moves_money_exactly_once_and_keeps_the_ledger_balanced()
    {
        using var client = factory.Client();
        var from = await factory.OpenAccountAsync(client, 100m);
        var to = await factory.OpenAccountAsync(client);

        var (response, payment) = await LedgerApiFactory.PayAsync(client, from, to, 40.25m, reference: "order-1");
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        Assert.Equal(PaymentStatus.Initiated, payment!.Status);

        await factory.DrainAsync();

        var settled = await LedgerApiFactory.GetAsync<PaymentResponse>(client, $"/api/v1/payments/{payment.PaymentId}");
        var source = await LedgerApiFactory.GetAsync<AccountResponse>(client, $"/api/v1/accounts/{from}");
        var destination = await LedgerApiFactory.GetAsync<AccountResponse>(client, $"/api/v1/accounts/{to}");
        var statement = await LedgerApiFactory.GetAsync<PagedResponse<StatementLine>>(client, $"/api/v1/accounts/{from}/statement");

        Assert.Equal(PaymentStatus.Completed, settled.Status);
        Assert.Equal((59.75m, 0m), (source.Balance, source.Held));
        Assert.Equal(40.25m, destination.Balance);
        Assert.Equal(2, statement.TotalCount);
        await AssertLedgerIsCleanAsync(client);
    }

    [Fact]
    public async Task Insufficient_funds_fail_the_payment_without_touching_balances()
    {
        using var client = factory.Client();
        var from = await factory.OpenAccountAsync(client, 10m);
        var to = await factory.OpenAccountAsync(client);

        var (_, payment) = await LedgerApiFactory.PayAsync(client, from, to, 10.01m);
        await factory.DrainAsync();

        var failed = await LedgerApiFactory.GetAsync<PaymentResponse>(client, $"/api/v1/payments/{payment!.PaymentId}");
        Assert.Equal((PaymentStatus.Failed, "Account.InsufficientFunds"), (failed.Status, failed.FailureCode));
        Assert.Equal(10m, (await LedgerApiFactory.GetAsync<AccountResponse>(client, $"/api/v1/accounts/{from}")).Available);
    }

    [Fact]
    public async Task A_risk_rejection_is_compensated_by_releasing_the_hold()
    {
        using var client = factory.Client();
        var from = await factory.OpenAccountAsync(client, 500m);
        var to = await factory.OpenAccountAsync(client);
        factory.Risk.RejectAmountsMinor[31_337] = "velocity";

        var (_, payment) = await LedgerApiFactory.PayAsync(client, from, to, 313.37m);
        await factory.DrainAsync();

        var failed = await LedgerApiFactory.GetAsync<PaymentResponse>(client, $"/api/v1/payments/{payment!.PaymentId}");
        var source = await LedgerApiFactory.GetAsync<AccountResponse>(client, $"/api/v1/accounts/{from}");
        Assert.Equal((PaymentStatus.Failed, "Risk.Rejected", 95), (failed.Status, failed.FailureCode, failed.RiskScore));
        Assert.Equal((500m, 0m), (source.Balance, source.Held));
        await AssertLedgerIsCleanAsync(client);
    }

    [Fact]
    public async Task A_destination_frozen_mid_flight_triggers_compensation()
    {
        using var client = factory.Client();
        var from = await factory.OpenAccountAsync(client, 100m);
        var to = await factory.OpenAccountAsync(client);

        var (_, payment) = await LedgerApiFactory.PayAsync(client, from, to, 20m);
        (await client.PostAsJsonAsync($"/api/v1/admin/accounts/{to}/freeze", new { reason = "Compliance hold" })).EnsureSuccessStatusCode();
        await factory.DrainAsync();

        var failed = await LedgerApiFactory.GetAsync<PaymentResponse>(client, $"/api/v1/payments/{payment!.PaymentId}");
        Assert.Equal((PaymentStatus.Failed, "Account.Frozen"), (failed.Status, failed.FailureCode));
        Assert.Equal((100m, 0m), Balance(await LedgerApiFactory.GetAsync<AccountResponse>(client, $"/api/v1/accounts/{from}")));
        await AssertLedgerIsCleanAsync(client);
    }

    [Fact]
    public async Task A_risk_engine_outage_keeps_funds_reserved_and_the_saga_resumes_after_recovery()
    {
        using var client = factory.Client();
        var from = await factory.OpenAccountAsync(client, 100m);
        var to = await factory.OpenAccountAsync(client);

        factory.Risk.Unavailable = true;
        var (_, payment) = await LedgerApiFactory.PayAsync(client, from, to, 30m);
        await factory.DrainAsync();

        var waiting = await LedgerApiFactory.GetAsync<PaymentResponse>(client, $"/api/v1/payments/{payment!.PaymentId}");
        var held = await LedgerApiFactory.GetAsync<AccountResponse>(client, $"/api/v1/accounts/{from}");
        Assert.Equal(PaymentStatus.FundsReserved, waiting.Status);
        Assert.Equal(30m, held.Held);

        // The failed step is retried with backoff once the engine is back.
        factory.Risk.Unavailable = false;
        factory.Clock.Advance(TimeSpan.FromMinutes(5));
        await factory.DrainAsync();

        Assert.Equal(PaymentStatus.Completed, (await LedgerApiFactory.GetAsync<PaymentResponse>(client, $"/api/v1/payments/{payment.PaymentId}")).Status);
    }

    [Fact]
    public async Task Stalled_payments_are_redriven_and_abandoned_ones_time_out()
    {
        using var client = factory.Client();
        var from = await factory.OpenAccountAsync(client, 100m);
        var to = await factory.OpenAccountAsync(client);
        var (_, stalled) = await LedgerApiFactory.PayAsync(client, from, to, 10m);
        var (_, abandoned) = await LedgerApiFactory.PayAsync(client, from, to, 15m);

        // Simulate lost messages (e.g. dead-lettered during an outage).
        await using (var scope = factory.Services.CreateAsyncScope())
        {
            await scope.ServiceProvider.GetRequiredService<LedgerDbContext>().Outbox
                .Where(m => m.ProcessedOnUtc == null)
                .ExecuteUpdateAsync(s => s.SetProperty(m => m.ProcessedOnUtc, DateTime.UtcNow));
        }

        await factory.DrainAsync();
        Assert.Equal(PaymentStatus.Initiated, (await LedgerApiFactory.GetAsync<PaymentResponse>(client, $"/api/v1/payments/{stalled!.PaymentId}")).Status);

        factory.Clock.Advance(TimeSpan.FromMinutes(3));
        var redrive = await Send(new RedriveStalledPaymentsCommand(TimeSpan.FromMinutes(2), TimeSpan.FromMinutes(30)));
        Assert.True(redrive.Redriven >= 2);
        await factory.DrainAsync();
        Assert.Equal(PaymentStatus.Completed, (await LedgerApiFactory.GetAsync<PaymentResponse>(client, $"/api/v1/payments/{stalled.PaymentId}")).Status);
        Assert.Equal(PaymentStatus.Completed, (await LedgerApiFactory.GetAsync<PaymentResponse>(client, $"/api/v1/payments/{abandoned!.PaymentId}")).Status);

        // A payment whose next step never succeeds is compensated after the hard timeout.
        factory.Risk.Unavailable = true;
        var (_, doomed) = await LedgerApiFactory.PayAsync(client, from, to, 5m);
        await factory.DrainAsync();
        factory.Clock.Advance(TimeSpan.FromMinutes(31));
        await Send(new RedriveStalledPaymentsCommand(TimeSpan.FromMinutes(2), TimeSpan.FromMinutes(30)));
        await factory.DrainAsync();
        factory.Risk.Unavailable = false;

        var timedOut = await LedgerApiFactory.GetAsync<PaymentResponse>(client, $"/api/v1/payments/{doomed!.PaymentId}");
        Assert.Equal((PaymentStatus.Failed, "Payment.Timeout"), (timedOut.Status, timedOut.FailureCode));
        Assert.Equal((75m, 0m), Balance(await LedgerApiFactory.GetAsync<AccountResponse>(client, $"/api/v1/accounts/{from}")));
    }

    [Fact]
    public async Task Hundreds_of_random_payments_leave_the_ledger_reconciled()
    {
        using var client = factory.Client();
        var random = new Random(2026);
        var accounts = new List<Guid>();
        for (var i = 0; i < 8; i++)
        {
            accounts.Add(await factory.OpenAccountAsync(client, random.Next(50, 1_000)));
        }

        var before = (await LedgerApiFactory.GetAsync<TrialBalanceResponse>(client, "/api/v1/admin/trial-balance")).Currencies.Single().CustomerTotal;
        for (var i = 0; i < 200; i++)
        {
            var from = accounts[random.Next(accounts.Count)];
            var to = accounts.Where(a => a != from).ElementAt(random.Next(accounts.Count - 1));
            await LedgerApiFactory.PayAsync(client, from, to, Math.Round((decimal)random.NextDouble() * 300m, 2) + 0.01m);
        }

        await factory.DrainAsync();

        var stats = await LedgerApiFactory.GetAsync<LedgerStatsResponse>(client, "/api/v1/admin/stats");
        var after = (await LedgerApiFactory.GetAsync<TrialBalanceResponse>(client, "/api/v1/admin/trial-balance")).Currencies.Single().CustomerTotal;
        Assert.Equal(before, after); // transfers never create or destroy money
        Assert.True(stats.PaymentsByStatus.GetValueOrDefault("Completed") > 0 && stats.PaymentsByStatus.GetValueOrDefault("Failed") > 0);
        await AssertLedgerIsCleanAsync(client);
    }

    private static (decimal Balance, decimal Held) Balance(AccountResponse account) => (account.Balance, account.Held);

    private async Task<RedriveResponse> Send(RedriveStalledPaymentsCommand command)
    {
        await using var scope = factory.Services.CreateAsyncScope();
        return (await scope.ServiceProvider.GetRequiredService<Application.Abstractions.Messaging.ISender>().Send(command)).Value;
    }

    private static async Task AssertLedgerIsCleanAsync(HttpClient client)
    {
        var report = await LedgerApiFactory.GetAsync<ReconciliationResponse>(client, "/api/v1/admin/reconciliation");
        Assert.True(report.IsClean, string.Join("; ", report.Issues.Select(i => $"{i.Kind} {i.Subject}: {i.Detail}")));
    }
}
