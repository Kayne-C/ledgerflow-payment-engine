using System.Collections.Concurrent;
using StackExchange.Redis;

namespace LedgerFlow.Risk;

public sealed class RiskRulesOptions
{
    public const string Section = "RiskRules";

    /// <summary>Single payments at or above this amount (minor units, per currency) add review risk.</summary>
    public Dictionary<string, long> LargeAmountMinor { get; set; } = new() { ["TRY"] = 10_000_000, ["USD"] = 500_000, ["EUR"] = 500_000 };

    /// <summary>Single payments at or above this amount are rejected outright.</summary>
    public Dictionary<string, long> BlockAmountMinor { get; set; } = new() { ["TRY"] = 25_000_000, ["USD"] = 1_000_000, ["EUR"] = 1_000_000 };

    /// <summary>Velocity rule: more payments than this from one account within the window is suspicious.</summary>
    public int MaxPaymentsPerWindow { get; set; } = 30;

    public TimeSpan VelocityWindow { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>Accounts that may neither send nor receive (sanctions / confirmed fraud).</summary>
    public HashSet<Guid> Watchlist { get; set; } = [];

    public int RejectAtScore { get; set; } = 80;
}

public sealed record RiskVerdict(bool Approved, int Score, string? Reason, IReadOnlyList<string> TriggeredRules);

/// <summary>
/// Sliding-window counter per account. Members are payment ids, so evaluating the same payment twice
/// (a retried saga step) never inflates the count.
/// </summary>
public interface IVelocityStore
{
    Task<long> RecordAndCountAsync(Guid accountId, Guid paymentId, DateTimeOffset now, TimeSpan window);
}

public sealed class RedisVelocityStore(IConnectionMultiplexer redis) : IVelocityStore
{
    public async Task<long> RecordAndCountAsync(Guid accountId, Guid paymentId, DateTimeOffset now, TimeSpan window)
    {
        var key = (RedisKey)$"risk:velocity:{accountId:N}";
        var nowMs = now.ToUnixTimeMilliseconds();
        var db = redis.GetDatabase();

        var transaction = db.CreateTransaction();
        _ = transaction.SortedSetAddAsync(key, paymentId.ToString("N"), nowMs);
        _ = transaction.SortedSetRemoveRangeByScoreAsync(key, double.NegativeInfinity, nowMs - window.TotalMilliseconds);
        var count = transaction.SortedSetLengthAsync(key);
        _ = transaction.KeyExpireAsync(key, window * 2);
        await transaction.ExecuteAsync();
        return await count;
    }
}

public sealed class InMemoryVelocityStore : IVelocityStore
{
    private readonly ConcurrentDictionary<Guid, ConcurrentDictionary<Guid, DateTimeOffset>> _windows = new();

    public Task<long> RecordAndCountAsync(Guid accountId, Guid paymentId, DateTimeOffset now, TimeSpan window)
    {
        var entries = _windows.GetOrAdd(accountId, _ => new ConcurrentDictionary<Guid, DateTimeOffset>());
        entries.TryAdd(paymentId, now);
        foreach (var (id, at) in entries)
        {
            if (at < now - window)
            {
                entries.TryRemove(id, out _);
            }
        }

        return Task.FromResult((long)entries.Count);
    }
}

/// <summary>Additive rule-based scoring: transparent, testable, and cheap enough to run inline in the saga.</summary>
public sealed class RiskRuleEngine(IVelocityStore velocity, Microsoft.Extensions.Options.IOptionsMonitor<RiskRulesOptions> options, TimeProvider clock)
{
    public async Task<RiskVerdict> EvaluateAsync(Guid paymentId, Guid fromAccountId, Guid toAccountId, long amountMinor, string currency)
    {
        var rules = options.CurrentValue;
        var triggered = new List<string>();
        var score = 0;

        if (rules.Watchlist.Contains(fromAccountId) || rules.Watchlist.Contains(toAccountId))
        {
            triggered.Add("watchlist");
            score += 100;
        }

        if (rules.BlockAmountMinor.TryGetValue(currency, out var block) && amountMinor >= block)
        {
            triggered.Add("amount.block");
            score += 100;
        }
        else if (rules.LargeAmountMinor.TryGetValue(currency, out var large) && amountMinor >= large)
        {
            triggered.Add("amount.large");
            score += 40;
        }

        var recent = await velocity.RecordAndCountAsync(fromAccountId, paymentId, clock.GetUtcNow(), rules.VelocityWindow);
        if (recent > rules.MaxPaymentsPerWindow)
        {
            triggered.Add("velocity");
            score += 80;
        }

        score = Math.Min(score, 100);
        var approved = score < rules.RejectAtScore;
        return new RiskVerdict(approved, score, approved ? null : $"Rejected by rules: {string.Join(", ", triggered)}.", triggered);
    }
}
