using LedgerFlow.Domain.Common;
using LedgerFlow.Domain.Values;

namespace LedgerFlow.Domain.Accounts;

public enum AccountKind
{
    /// <summary>Customer wallet; can never go below zero available balance.</summary>
    Customer,

    /// <summary>System account mirroring an external rail (bank, card scheme). May be negative: it is the other leg of deposits.</summary>
    Clearing,
}

public enum AccountStatus
{
    Active,
    Frozen,
}

public sealed record AccountOpened(Guid AccountId, string Holder, string Currency, AccountKind Kind, DateTime OpenedAtUtc) : IDomainEvent;

public sealed record FundsHeld(Guid AccountId, Guid HoldId, long AmountMinor) : IDomainEvent;

public sealed record HoldReleased(Guid AccountId, Guid HoldId, long AmountMinor) : IDomainEvent;

public sealed record AccountDebited(Guid AccountId, Guid TransactionId, long AmountMinor, long BalanceAfterMinor, Guid? CapturedHoldId) : IDomainEvent;

public sealed record AccountCredited(Guid AccountId, Guid TransactionId, long AmountMinor, long BalanceAfterMinor) : IDomainEvent;

public sealed record AccountFrozen(Guid AccountId, string Reason) : IDomainEvent;

public sealed record AccountUnfrozen(Guid AccountId) : IDomainEvent;

public sealed record AccountSnapshot(
    Guid AccountId,
    string Holder,
    string Currency,
    AccountKind Kind,
    AccountStatus Status,
    long BalanceMinor,
    IReadOnlyDictionary<Guid, long> Holds,
    DateTime OpenedAtUtc);

/// <summary>
/// Ledger account. Balance changes only through debits/credits that belong to a balanced
/// <see cref="Transactions.LedgerTransaction"/>; holds reserve funds for in-flight payments.
/// </summary>
public sealed class Account : EventSourcedAggregate, IEventSourced<Account>
{
    public const int HolderMaxLength = 128;

    private readonly Dictionary<Guid, long> _holds = [];

    private Account()
    {
    }

    public override string StreamId => StreamName(Id);

    public string Holder { get; private set; } = string.Empty;

    public string Currency { get; private set; } = string.Empty;

    public AccountKind Kind { get; private set; }

    public AccountStatus Status { get; private set; }

    public long BalanceMinor { get; private set; }

    public long HeldMinor => _holds.Values.Sum();

    public long AvailableMinor => BalanceMinor - HeldMinor;

    public DateTime OpenedAtUtc { get; private set; }

    public IReadOnlyDictionary<Guid, long> Holds => _holds;

    public static string StreamName(Guid id) => $"account-{id:N}";

    public static Account CreateEmpty() => new();

    public static Result<Account> Open(Guid id, string holder, Currency currency, AccountKind kind, DateTime utcNow)
    {
        if (string.IsNullOrWhiteSpace(holder) || holder.Length > HolderMaxLength)
        {
            return AccountErrors.InvalidHolder;
        }

        var account = new Account();
        account.Raise(new AccountOpened(id, holder.Trim(), currency.Code, kind, utcNow));
        return account;
    }

    /// <summary>Reserves funds for a payment. Idempotent per hold id, so a redelivered command is harmless.</summary>
    public Result PlaceHold(Guid holdId, Money amount)
    {
        if (_holds.ContainsKey(holdId))
        {
            return Result.Success();
        }

        var check = CanDebit(amount);
        if (check.IsFailure)
        {
            return check;
        }

        Raise(new FundsHeld(Id, holdId, amount.MinorUnits));
        return Result.Success();
    }

    /// <summary>Compensation: gives reserved funds back. Idempotent — releasing an unknown hold is a no-op.</summary>
    public void ReleaseHold(Guid holdId)
    {
        if (_holds.TryGetValue(holdId, out var amount))
        {
            Raise(new HoldReleased(Id, holdId, amount));
        }
    }

    /// <summary>Turns a hold into a real debit (settlement). The amount was validated when the hold was placed.</summary>
    public Result CaptureHold(Guid holdId, Guid transactionId)
    {
        if (!_holds.TryGetValue(holdId, out var amount))
        {
            return AccountErrors.HoldNotFound(holdId);
        }

        Raise(new AccountDebited(Id, transactionId, amount, BalanceMinor - amount, holdId));
        return Result.Success();
    }

    public Result Debit(Guid transactionId, Money amount)
    {
        var check = CanDebit(amount);
        if (check.IsFailure)
        {
            return check;
        }

        Raise(new AccountDebited(Id, transactionId, amount.MinorUnits, BalanceMinor - amount.MinorUnits, null));
        return Result.Success();
    }

    public Result Credit(Guid transactionId, Money amount)
    {
        var check = CanMove(amount);
        if (check.IsFailure)
        {
            return check;
        }

        Raise(new AccountCredited(Id, transactionId, amount.MinorUnits, BalanceMinor + amount.MinorUnits));
        return Result.Success();
    }

    public Result Freeze(string reason)
    {
        if (Kind == AccountKind.Clearing)
        {
            return AccountErrors.ClearingAccountsCannotBeFrozen;
        }

        if (Status != AccountStatus.Frozen)
        {
            Raise(new AccountFrozen(Id, reason));
        }

        return Result.Success();
    }

    public void Unfreeze()
    {
        if (Status == AccountStatus.Frozen)
        {
            Raise(new AccountUnfrozen(Id));
        }
    }

    public AccountSnapshot ToSnapshot() =>
        new(Id, Holder, Currency, Kind, Status, BalanceMinor, new Dictionary<Guid, long>(_holds), OpenedAtUtc);

    public static Account FromSnapshot(AccountSnapshot snapshot, long version, string? lastHash)
    {
        var account = new Account
        {
            Id = snapshot.AccountId,
            Holder = snapshot.Holder,
            Currency = snapshot.Currency,
            Kind = snapshot.Kind,
            Status = snapshot.Status,
            BalanceMinor = snapshot.BalanceMinor,
            OpenedAtUtc = snapshot.OpenedAtUtc,
        };

        foreach (var (holdId, amount) in snapshot.Holds)
        {
            account._holds[holdId] = amount;
        }

        account.RestoreVersion(version, lastHash);
        return account;
    }

    protected override void Apply(IDomainEvent domainEvent)
    {
        switch (domainEvent)
        {
            case AccountOpened e:
                Id = e.AccountId;
                Holder = e.Holder;
                Currency = e.Currency;
                Kind = e.Kind;
                Status = AccountStatus.Active;
                OpenedAtUtc = e.OpenedAtUtc;
                break;
            case FundsHeld e:
                _holds[e.HoldId] = e.AmountMinor;
                break;
            case HoldReleased e:
                _holds.Remove(e.HoldId);
                break;
            case AccountDebited e:
                BalanceMinor -= e.AmountMinor;
                if (e.CapturedHoldId is { } holdId)
                {
                    _holds.Remove(holdId);
                }

                break;
            case AccountCredited e:
                BalanceMinor += e.AmountMinor;
                break;
            case AccountFrozen:
                Status = AccountStatus.Frozen;
                break;
            case AccountUnfrozen:
                Status = AccountStatus.Active;
                break;
            default:
                throw new InvalidOperationException($"Account cannot apply {domainEvent.GetType().Name}.");
        }
    }

    private Result CanMove(Money amount)
    {
        if (Status == AccountStatus.Frozen)
        {
            return AccountErrors.Frozen(Id);
        }

        if (amount.Currency.Code != Currency)
        {
            return MoneyErrors.CurrencyMismatch(Currency, amount.Currency.Code);
        }

        return amount.IsPositive ? Result.Success() : MoneyErrors.NotPositive;
    }

    private Result CanDebit(Money amount)
    {
        var check = CanMove(amount);
        if (check.IsFailure)
        {
            return check;
        }

        // Clearing accounts are the external leg of deposits and may legitimately go negative.
        return Kind == AccountKind.Customer && AvailableMinor < amount.MinorUnits
            ? AccountErrors.InsufficientFunds(Id)
            : Result.Success();
    }
}

public static class AccountErrors
{
    public static readonly Error InvalidHolder =
        Error.BusinessRule("Account.InvalidHolder", $"Holder is required and may not exceed {Account.HolderMaxLength} characters.");

    public static readonly Error ClearingAccountsCannotBeFrozen =
        Error.BusinessRule("Account.ClearingNotFreezable", "Clearing accounts cannot be frozen.");

    public static Error NotFound(Guid id) => Error.NotFound("Account.NotFound", $"Account '{id}' was not found.");

    public static Error InsufficientFunds(Guid id) =>
        Error.BusinessRule("Account.InsufficientFunds", $"Account '{id}' does not have enough available funds.");

    public static Error Frozen(Guid id) => Error.BusinessRule("Account.Frozen", $"Account '{id}' is frozen.");

    public static Error HoldNotFound(Guid holdId) => Error.BusinessRule("Account.HoldNotFound", $"Hold '{holdId}' does not exist.");
}
