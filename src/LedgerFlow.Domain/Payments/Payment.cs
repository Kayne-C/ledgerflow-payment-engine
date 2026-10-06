using LedgerFlow.Domain.Common;
using LedgerFlow.Domain.Values;

namespace LedgerFlow.Domain.Payments;

/// <summary>
/// Saga state. Happy path: Initiated → FundsReserved → RiskApproved → Completed.
/// Failures before a hold is placed end in Failed; after a hold, the funds are released first (compensation).
/// </summary>
public enum PaymentStatus
{
    Initiated,
    FundsReserved,
    RiskApproved,
    RiskRejected,
    Completed,
    Failed,
}

public sealed record PaymentInitiated(
    Guid PaymentId,
    string ClientId,
    string IdempotencyKey,
    string RequestFingerprint,
    Guid FromAccountId,
    Guid ToAccountId,
    long AmountMinor,
    string Currency,
    string? Reference,
    DateTime InitiatedAtUtc) : IDomainEvent;

public sealed record PaymentFundsReserved(Guid PaymentId) : IDomainEvent;

public sealed record PaymentRiskAssessed(Guid PaymentId, bool Approved, int Score, string? Reason) : IDomainEvent;

public sealed record PaymentCompleted(Guid PaymentId, Guid TransactionId, DateTime CompletedAtUtc) : IDomainEvent;

public sealed record PaymentFailed(Guid PaymentId, string ReasonCode, string Reason, DateTime FailedAtUtc) : IDomainEvent;

public sealed class Payment : EventSourcedAggregate, IEventSourced<Payment>
{
    public const int ReferenceMaxLength = 140;

    private Payment()
    {
    }

    public override string StreamId => StreamName(Id);

    public string ClientId { get; private set; } = string.Empty;

    public string IdempotencyKey { get; private set; } = string.Empty;

    /// <summary>Hash of the business fields; detects an idempotency key reused for a different request.</summary>
    public string RequestFingerprint { get; private set; } = string.Empty;

    public Guid FromAccountId { get; private set; }

    public Guid ToAccountId { get; private set; }

    public long AmountMinor { get; private set; }

    public string Currency { get; private set; } = string.Empty;

    public string? Reference { get; private set; }

    public PaymentStatus Status { get; private set; }

    public int? RiskScore { get; private set; }

    public string? FailureCode { get; private set; }

    public string? FailureReason { get; private set; }

    public Guid? TransactionId { get; private set; }

    public DateTime InitiatedAtUtc { get; private set; }

    public DateTime? FinishedAtUtc { get; private set; }

    public bool IsTerminal => Status is PaymentStatus.Completed or PaymentStatus.Failed;

    /// <summary>Funds are held on the source account while the payment is in one of these states.</summary>
    public bool HoldsFunds => Status is PaymentStatus.FundsReserved or PaymentStatus.RiskApproved or PaymentStatus.RiskRejected;

    public Money Amount => new(AmountMinor, Values.Currency.From(Currency).Value);

    public static string StreamName(Guid id) => $"payment-{id:N}";

    public static Payment CreateEmpty() => new();

    public static Result<Payment> Initiate(
        Guid paymentId,
        string clientId,
        string idempotencyKey,
        string requestFingerprint,
        Guid fromAccountId,
        Guid toAccountId,
        Money amount,
        string? reference,
        DateTime utcNow)
    {
        if (fromAccountId == toAccountId)
        {
            return PaymentErrors.SameAccount;
        }

        if (!amount.IsPositive)
        {
            return MoneyErrors.NotPositive;
        }

        if (reference?.Length > ReferenceMaxLength)
        {
            return PaymentErrors.ReferenceTooLong;
        }

        var payment = new Payment();
        payment.Raise(new PaymentInitiated(
            paymentId, clientId, idempotencyKey, requestFingerprint, fromAccountId, toAccountId,
            amount.MinorUnits, amount.Currency.Code, reference, utcNow));
        return payment;
    }

    public Result MarkFundsReserved() => Transition(PaymentStatus.Initiated, new PaymentFundsReserved(Id));

    public Result RecordRiskDecision(bool approved, int score, string? reason) =>
        Transition(PaymentStatus.FundsReserved, new PaymentRiskAssessed(Id, approved, score, reason));

    public Result Complete(Guid transactionId, DateTime utcNow) =>
        Transition(PaymentStatus.RiskApproved, new PaymentCompleted(Id, transactionId, utcNow));

    /// <summary>Terminal failure. Callers must release any hold first (see <see cref="HoldsFunds"/>).</summary>
    public Result Fail(string reasonCode, string reason, DateTime utcNow)
    {
        if (IsTerminal)
        {
            return PaymentErrors.InvalidTransition(Status, PaymentStatus.Failed);
        }

        Raise(new PaymentFailed(Id, reasonCode, reason, utcNow));
        return Result.Success();
    }

    protected override void Apply(IDomainEvent domainEvent)
    {
        switch (domainEvent)
        {
            case PaymentInitiated e:
                Id = e.PaymentId;
                ClientId = e.ClientId;
                IdempotencyKey = e.IdempotencyKey;
                RequestFingerprint = e.RequestFingerprint;
                FromAccountId = e.FromAccountId;
                ToAccountId = e.ToAccountId;
                AmountMinor = e.AmountMinor;
                Currency = e.Currency;
                Reference = e.Reference;
                InitiatedAtUtc = e.InitiatedAtUtc;
                Status = PaymentStatus.Initiated;
                break;
            case PaymentFundsReserved:
                Status = PaymentStatus.FundsReserved;
                break;
            case PaymentRiskAssessed e:
                RiskScore = e.Score;
                Status = e.Approved ? PaymentStatus.RiskApproved : PaymentStatus.RiskRejected;
                if (!e.Approved)
                {
                    FailureCode = "Risk.Rejected";
                    FailureReason = e.Reason;
                }

                break;
            case PaymentCompleted e:
                TransactionId = e.TransactionId;
                FinishedAtUtc = e.CompletedAtUtc;
                Status = PaymentStatus.Completed;
                break;
            case PaymentFailed e:
                FailureCode = e.ReasonCode;
                FailureReason = e.Reason;
                FinishedAtUtc = e.FailedAtUtc;
                Status = PaymentStatus.Failed;
                break;
            default:
                throw new InvalidOperationException($"Payment cannot apply {domainEvent.GetType().Name}.");
        }
    }

    private Result Transition(PaymentStatus requiredStatus, IDomainEvent domainEvent)
    {
        if (Status != requiredStatus)
        {
            return PaymentErrors.InvalidTransition(Status, requiredStatus);
        }

        Raise(domainEvent);
        return Result.Success();
    }
}

public static class PaymentErrors
{
    public static readonly Error SameAccount =
        Error.BusinessRule("Payment.SameAccount", "Source and destination accounts must differ.");

    public static readonly Error ReferenceTooLong =
        Error.BusinessRule("Payment.ReferenceTooLong", $"Reference may not exceed {Payment.ReferenceMaxLength} characters.");

    public static readonly Error IdempotencyKeyReused = Error.BusinessRule(
        "Idempotency.KeyReused", "This Idempotency-Key was already used for a different request.");

    public static Error NotFound(Guid id) => Error.NotFound("Payment.NotFound", $"Payment '{id}' was not found.");

    public static Error InvalidTransition(PaymentStatus current, PaymentStatus expected) => Error.Conflict(
        "Payment.InvalidTransition", $"Payment is {current}; this step requires {expected}.");
}
