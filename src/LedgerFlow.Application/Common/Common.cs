using LedgerFlow.Application.Abstractions;
using LedgerFlow.Application.Diagnostics;
using LedgerFlow.Domain.Common;

namespace LedgerFlow.Application.Common;

public sealed record PagedResponse<T>(IReadOnlyList<T> Items, int Page, int PageSize, int TotalCount);

public static class Paging
{
    public const int MaxPageSize = 200;
}

/// <summary>
/// Optimistic concurrency loop: reload, re-decide, re-append. Each attempt starts from a clean session, so the
/// business decision (e.g. "is there enough money?") is always taken against the latest committed state.
/// </summary>
internal static class Optimistic
{
    public const int MaxAttempts = 15;
    private const int BaseDelayMs = 4;
    private const int MaxDelayMs = 250;

    public static async Task<TResult> RetryAsync<TResult>(
        ILedgerSession session,
        Func<CancellationToken, Task<TResult>> attempt,
        CancellationToken cancellationToken)
    {
        for (var attemptNumber = 1; ; attemptNumber++)
        {
            try
            {
                return await attempt(cancellationToken);
            }
            catch (ConcurrencyConflictException) when (attemptNumber < MaxAttempts)
            {
                LedgerTelemetry.ConcurrencyConflicts.Add(1);
                session.Reset();

                // Exponential backoff with full jitter: competing writers on a hot account spread out
                // instead of colliding again in lockstep.
                var ceiling = Math.Min(MaxDelayMs, BaseDelayMs << Math.Min(attemptNumber, 6));
                await Task.Delay(Random.Shared.Next(1, ceiling + 1), cancellationToken);
            }
        }
    }
}

public static class ClearingAccounts
{
    /// <summary>One system clearing account per currency, with a stable id derived from the currency code.</summary>
    public static Guid IdFor(string currency) => Deterministic.Id("clearing-account", "system", currency);
}

public static class IdempotencyErrors
{
    public static readonly Error MissingKey = new ValidationError(new Dictionary<string, string[]>
    {
        ["Idempotency-Key"] = ["The Idempotency-Key header is required for money movements."],
    });
}
