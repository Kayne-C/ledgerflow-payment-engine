using LedgerFlow.Domain.Common;
using LedgerFlow.Domain.Values;

namespace LedgerFlow.Domain.Transactions;

public enum TransactionKind
{
    Deposit,
    Withdrawal,
    Payment,
}

public enum PostingDirection
{
    Debit,
    Credit,
}

public sealed record Posting(Guid AccountId, PostingDirection Direction, long AmountMinor);

public sealed record TransactionPosted(
    Guid TransactionId,
    TransactionKind Kind,
    string Currency,
    IReadOnlyList<Posting> Postings,
    string RequestFingerprint,
    string? Reference,
    DateTime PostedAtUtc) : IDomainEvent;

/// <summary>
/// Journal entry of the double-entry ledger. Its stream holds exactly one event and is always appended with
/// "no stream" as the expected version, so the database's unique index guarantees that a transaction
/// (e.g. a retried deposit) can be posted at most once.
/// </summary>
public sealed class LedgerTransaction : EventSourcedAggregate, IEventSourced<LedgerTransaction>
{
    private readonly List<Posting> _postings = [];

    private LedgerTransaction()
    {
    }

    public override string StreamId => StreamName(Id);

    public TransactionKind Kind { get; private set; }

    public string Currency { get; private set; } = string.Empty;

    public string RequestFingerprint { get; private set; } = string.Empty;

    public string? Reference { get; private set; }

    public DateTime PostedAtUtc { get; private set; }

    public IReadOnlyList<Posting> Postings => _postings;

    public static string StreamName(Guid id) => $"txn-{id:N}";

    public static LedgerTransaction CreateEmpty() => new();

    /// <summary>Invariant: at least two legs, one currency, positive amounts, and Σ debits = Σ credits.</summary>
    public static Result<LedgerTransaction> Post(
        Guid transactionId,
        TransactionKind kind,
        Currency currency,
        IReadOnlyList<Posting> postings,
        string requestFingerprint,
        string? reference,
        DateTime utcNow)
    {
        if (postings.Count < 2)
        {
            return TransactionErrors.TooFewPostings;
        }

        if (postings.Any(p => p.AmountMinor <= 0))
        {
            return MoneyErrors.NotPositive;
        }

        var debits = postings.Where(p => p.Direction == PostingDirection.Debit).Sum(p => p.AmountMinor);
        var credits = postings.Where(p => p.Direction == PostingDirection.Credit).Sum(p => p.AmountMinor);
        if (debits != credits)
        {
            return TransactionErrors.Unbalanced(debits, credits);
        }

        var transaction = new LedgerTransaction();
        transaction.Raise(new TransactionPosted(transactionId, kind, currency.Code, postings, requestFingerprint, reference, utcNow));
        return transaction;
    }

    protected override void Apply(IDomainEvent domainEvent)
    {
        if (domainEvent is not TransactionPosted e)
        {
            throw new InvalidOperationException($"Transaction cannot apply {domainEvent.GetType().Name}.");
        }

        Id = e.TransactionId;
        Kind = e.Kind;
        Currency = e.Currency;
        RequestFingerprint = e.RequestFingerprint;
        Reference = e.Reference;
        PostedAtUtc = e.PostedAtUtc;
        _postings.AddRange(e.Postings);
    }
}

public static class TransactionErrors
{
    public static readonly Error TooFewPostings =
        Error.BusinessRule("Transaction.TooFewPostings", "A transaction needs at least one debit and one credit.");

    public static Error Unbalanced(long debits, long credits) =>
        Error.BusinessRule("Transaction.Unbalanced", $"Debits ({debits}) must equal credits ({credits}).");

    public static Error NotFound(Guid id) => Error.NotFound("Transaction.NotFound", $"Transaction '{id}' was not found.");
}
