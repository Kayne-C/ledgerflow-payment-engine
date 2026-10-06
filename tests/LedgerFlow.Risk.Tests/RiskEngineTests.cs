using Grpc.Core;
using Grpc.Net.Client;
using LedgerFlow.Contracts.Risk;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;

namespace LedgerFlow.Risk.Tests;

public sealed class RiskRuleEngineTests
{
    private readonly FakeTimeProvider _clock = new(new DateTimeOffset(2026, 5, 1, 10, 0, 0, TimeSpan.Zero));

    private RiskRuleEngine Engine(Action<RiskRulesOptions>? configure = null)
    {
        var options = new RiskRulesOptions { MaxPaymentsPerWindow = 3 };
        configure?.Invoke(options);
        return new RiskRuleEngine(new InMemoryVelocityStore(), new StaticOptions(options), _clock);
    }

    [Fact]
    public async Task Ordinary_payments_are_approved_with_a_low_score()
    {
        var verdict = await Engine().EvaluateAsync(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), 15_000, "TRY");

        Assert.True(verdict.Approved);
        Assert.Equal(0, verdict.Score);
    }

    [Fact]
    public async Task Large_payments_raise_the_score_and_blocked_amounts_are_rejected()
    {
        var engine = Engine();

        var large = await engine.EvaluateAsync(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), 10_000_000, "TRY");
        var blocked = await engine.EvaluateAsync(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), 25_000_000, "TRY");

        Assert.True(large.Approved);
        Assert.Equal(["amount.large"], large.TriggeredRules);
        Assert.False(blocked.Approved);
        Assert.Contains("amount.block", blocked.TriggeredRules);
    }

    [Fact]
    public async Task Watchlisted_counterparties_are_rejected()
    {
        var flagged = Guid.NewGuid();
        var engine = Engine(o => o.Watchlist.Add(flagged));

        var verdict = await engine.EvaluateAsync(Guid.NewGuid(), Guid.NewGuid(), flagged, 100, "TRY");

        Assert.False(verdict.Approved);
        Assert.Equal(100, verdict.Score);
    }

    [Fact]
    public async Task Velocity_is_counted_per_payment_so_retries_do_not_inflate_it()
    {
        var engine = Engine();
        var account = Guid.NewGuid();
        var retried = Guid.NewGuid();

        for (var i = 0; i < 5; i++)
        {
            Assert.True((await engine.EvaluateAsync(retried, account, Guid.NewGuid(), 100, "TRY")).Approved);
        }

        for (var i = 0; i < 2; i++)
        {
            Assert.True((await engine.EvaluateAsync(Guid.NewGuid(), account, Guid.NewGuid(), 100, "TRY")).Approved);
        }

        var burst = await engine.EvaluateAsync(Guid.NewGuid(), account, Guid.NewGuid(), 100, "TRY");
        Assert.False(burst.Approved);
        Assert.Contains("velocity", burst.TriggeredRules);

        _clock.Advance(TimeSpan.FromMinutes(2));
        Assert.True((await engine.EvaluateAsync(Guid.NewGuid(), account, Guid.NewGuid(), 100, "TRY")).Approved);
    }

    private sealed class StaticOptions(RiskRulesOptions value) : IOptionsMonitor<RiskRulesOptions>
    {
        public RiskRulesOptions CurrentValue => value;

        public RiskRulesOptions Get(string? name) => value;

        public IDisposable? OnChange(Action<RiskRulesOptions, string?> listener) => null;
    }
}

public sealed class RiskGrpcEndpointTests(WebApplicationFactory<Program> factory) : IClassFixture<WebApplicationFactory<Program>>
{
    [Fact]
    public async Task The_engine_answers_over_grpc_and_validates_its_input()
    {
        using var channel = GrpcChannel.ForAddress(factory.Server.BaseAddress, new GrpcChannelOptions { HttpHandler = factory.Server.CreateHandler() });
        var client = new RiskEngine.RiskEngineClient(channel);

        var reply = await client.AssessAsync(new AssessRequest
        {
            PaymentId = Guid.NewGuid().ToString(),
            FromAccountId = Guid.NewGuid().ToString(),
            ToAccountId = Guid.NewGuid().ToString(),
            AmountMinor = 12_345,
            Currency = "TRY",
        });
        var invalid = await Assert.ThrowsAsync<RpcException>(async () => await client.AssessAsync(new AssessRequest { PaymentId = "x" }));

        Assert.True(reply.Approved);
        Assert.Equal(StatusCode.InvalidArgument, invalid.StatusCode);
    }
}
