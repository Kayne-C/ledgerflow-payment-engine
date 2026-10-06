using LedgerFlow.Application.Abstractions;
using LedgerFlow.Contracts.Risk;

namespace LedgerFlow.Infrastructure.Risk;

public enum RiskMode
{
    /// <summary>Calls the LedgerFlow.Risk gRPC service (production).</summary>
    Grpc,

    /// <summary>Approves everything; single-process local development only.</summary>
    ApproveAll,
}

public sealed class RiskOptions
{
    public const string Section = "Risk";

    public RiskMode Mode { get; set; } = RiskMode.ApproveAll;

    public Uri Address { get; set; } = new("http://localhost:5091");

    public TimeSpan AttemptTimeout { get; set; } = TimeSpan.FromSeconds(2);

    public TimeSpan TotalTimeout { get; set; } = TimeSpan.FromSeconds(10);
}

/// <summary>
/// gRPC client behind the standard resilience pipeline (Polly v8): per-attempt timeout, jittered retries and a
/// circuit breaker. When the engine is down the call fails fast, the saga step is retried later by the consumer,
/// and the funds simply stay reserved — nothing is approved by default.
/// </summary>
internal sealed class GrpcRiskAssessor(RiskEngine.RiskEngineClient client) : IRiskAssessor
{
    public async Task<RiskDecision> AssessAsync(RiskRequest request, CancellationToken cancellationToken)
    {
        var reply = await client.AssessAsync(
            new AssessRequest
            {
                PaymentId = request.PaymentId.ToString(),
                FromAccountId = request.FromAccountId.ToString(),
                ToAccountId = request.ToAccountId.ToString(),
                AmountMinor = request.AmountMinor,
                Currency = request.Currency,
            },
            cancellationToken: cancellationToken);

        return new RiskDecision(reply.Approved, reply.Score, string.IsNullOrEmpty(reply.Reason) ? null : reply.Reason);
    }
}

internal sealed class ApproveAllRiskAssessor : IRiskAssessor
{
    public Task<RiskDecision> AssessAsync(RiskRequest request, CancellationToken cancellationToken) =>
        Task.FromResult(new RiskDecision(true, 0, null));
}
