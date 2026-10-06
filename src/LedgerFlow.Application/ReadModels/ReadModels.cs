using LedgerFlow.Domain.Accounts;
using LedgerFlow.Domain.Payments;
using LedgerFlow.Domain.Transactions;

namespace LedgerFlow.Application.ReadModels;

// Query-side projections, written in the same transaction as the events (strongly consistent reads).
// They are disposable: every row can be rebuilt by replaying the event store.

public sealed class AccountView
{
    public Guid AccountId { get; set; }

    public string Holder { get; set; } = string.Empty;

    public string Currency { get; set; } = string.Empty;

    public AccountKind Kind { get; set; }

    public AccountStatus Status { get; set; }

    public long BalanceMinor { get; set; }

    public long HeldMinor { get; set; }

    public long Version { get; set; }

    public DateTime OpenedAtUtc { get; set; }

    public DateTime UpdatedAtUtc { get; set; }
}

public sealed class LedgerEntryView
{
    public long Id { get; set; }

    public Guid TransactionId { get; set; }

    public Guid AccountId { get; set; }

    public PostingDirection Direction { get; set; }

    public long AmountMinor { get; set; }

    public long BalanceAfterMinor { get; set; }

    public string Currency { get; set; } = string.Empty;

    public DateTime PostedAtUtc { get; set; }
}

public sealed class PaymentView
{
    public Guid PaymentId { get; set; }

    public string ClientId { get; set; } = string.Empty;

    public Guid FromAccountId { get; set; }

    public Guid ToAccountId { get; set; }

    public long AmountMinor { get; set; }

    public string Currency { get; set; } = string.Empty;

    public string? Reference { get; set; }

    public PaymentStatus Status { get; set; }

    public int? RiskScore { get; set; }

    public string? FailureCode { get; set; }

    public string? FailureReason { get; set; }

    public DateTime InitiatedAtUtc { get; set; }

    public DateTime UpdatedAtUtc { get; set; }

    public DateTime? FinishedAtUtc { get; set; }
}
