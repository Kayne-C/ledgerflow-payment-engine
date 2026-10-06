using FluentValidation;
using LedgerFlow.Application.Abstractions;
using LedgerFlow.Application.Abstractions.Messaging;
using LedgerFlow.Application.Common;
using LedgerFlow.Domain.Accounts;
using LedgerFlow.Domain.Common;
using LedgerFlow.Domain.Transactions;
using LedgerFlow.Domain.Values;

namespace LedgerFlow.Application.Features.Accounts;

public sealed record AccountResponse(
    Guid AccountId,
    string Holder,
    string Currency,
    AccountKind Kind,
    AccountStatus Status,
    decimal Balance,
    decimal Held,
    decimal Available,
    long Version);

public sealed record OpenAccountCommand(string Holder, string Currency) : ICommand<AccountResponse>;

internal sealed class OpenAccountValidator : AbstractValidator<OpenAccountCommand>
{
    public OpenAccountValidator()
    {
        RuleFor(c => c.Holder).NotEmpty().MaximumLength(Account.HolderMaxLength);
        RuleFor(c => c.Currency).NotEmpty().Length(3);
    }
}

internal sealed class OpenAccountHandler(ILedgerSession session, TimeProvider clock) : ICommandHandler<OpenAccountCommand, AccountResponse>
{
    public async Task<Result<AccountResponse>> Handle(OpenAccountCommand command, CancellationToken cancellationToken)
    {
        var currency = Currency.From(command.Currency);
        if (currency.IsFailure)
        {
            return currency.Error;
        }

        var opened = Account.Open(Guid.CreateVersion7(), command.Holder, currency.Value, AccountKind.Customer, clock.GetUtcNow().UtcDateTime);
        if (opened.IsFailure)
        {
            return opened.Error;
        }

        session.Track(opened.Value);
        await session.CommitAsync(cancellationToken);
        return opened.Value.ToResponse();
    }
}

public sealed record FreezeAccountCommand(Guid AccountId, string Reason) : ICommand<AccountResponse>;

internal sealed class FreezeAccountValidator : AbstractValidator<FreezeAccountCommand>
{
    public FreezeAccountValidator() => RuleFor(c => c.Reason).NotEmpty().MaximumLength(200);
}

internal sealed class FreezeAccountHandler(ILedgerSession session) : ICommandHandler<FreezeAccountCommand, AccountResponse>
{
    public Task<Result<AccountResponse>> Handle(FreezeAccountCommand command, CancellationToken cancellationToken) =>
        Optimistic.RetryAsync<Result<AccountResponse>>(session, async ct =>
        {
            var account = await session.LoadAsync<Account>(command.AccountId, ct);
            if (account is null)
            {
                return AccountErrors.NotFound(command.AccountId);
            }

            var frozen = account.Freeze(command.Reason);
            if (frozen.IsFailure)
            {
                return frozen.Error;
            }

            session.Track(account);
            await session.CommitAsync(ct);
            return account.ToResponse();
        }, cancellationToken);
}

public sealed record TransactionResponse(
    Guid TransactionId,
    TransactionKind Kind,
    Guid AccountId,
    decimal Amount,
    string Currency,
    DateTime PostedAtUtc,
    bool Replayed);

/// <summary>Money in (from the clearing account) or out (to it). Both legs plus the journal entry commit atomically.</summary>
public sealed record MoveFundsCommand(
    TransactionKind Kind,
    string ClientId,
    string IdempotencyKey,
    Guid AccountId,
    decimal Amount,
    string Currency,
    string? Reference) : ICommand<TransactionResponse>;

internal sealed class MoveFundsValidator : AbstractValidator<MoveFundsCommand>
{
    public MoveFundsValidator()
    {
        RuleFor(c => c.Kind).Must(k => k is TransactionKind.Deposit or TransactionKind.Withdrawal);
        RuleFor(c => c.IdempotencyKey).NotEmpty().MaximumLength(100);
        RuleFor(c => c.Amount).GreaterThan(0);
        RuleFor(c => c.Currency).NotEmpty().Length(3);
        RuleFor(c => c.Reference).MaximumLength(140);
    }
}

internal sealed class MoveFundsHandler(ILedgerSession session, TimeProvider clock) : ICommandHandler<MoveFundsCommand, TransactionResponse>
{
    public async Task<Result<TransactionResponse>> Handle(MoveFundsCommand command, CancellationToken cancellationToken)
    {
        var amount = Money.FromMajor(command.Amount, command.Currency);
        if (amount.IsFailure)
        {
            return amount.Error;
        }

        var transactionId = Deterministic.Id(command.Kind.ToString(), command.ClientId, command.IdempotencyKey);
        var fingerprint = Deterministic.Fingerprint(command.Kind, command.AccountId, amount.Value.MinorUnits, amount.Value.Currency.Code, command.Reference);

        return await Optimistic.RetryAsync<Result<TransactionResponse>>(session, async ct =>
        {
            var existing = await session.LoadAsync<LedgerTransaction>(transactionId, ct);
            if (existing is not null)
            {
                return existing.RequestFingerprint == fingerprint
                    ? Response(existing, command, replayed: true)
                    : Domain.Payments.PaymentErrors.IdempotencyKeyReused;
            }

            var account = await session.LoadAsync<Account>(command.AccountId, ct);
            if (account is null || account.Kind != AccountKind.Customer)
            {
                return AccountErrors.NotFound(command.AccountId);
            }

            var clearingId = ClearingAccounts.IdFor(amount.Value.Currency.Code);
            var clearing = await session.LoadAsync<Account>(clearingId, ct)
                ?? Account.Open(clearingId, $"Clearing {amount.Value.Currency.Code}", amount.Value.Currency, AccountKind.Clearing, clock.GetUtcNow().UtcDateTime).Value;

            var (debited, credited) = command.Kind == TransactionKind.Deposit ? (clearing, account) : (account, clearing);
            var debit = debited.Debit(transactionId, amount.Value);
            if (debit.IsFailure)
            {
                return debit.Error;
            }

            var credit = credited.Credit(transactionId, amount.Value);
            if (credit.IsFailure)
            {
                return credit.Error;
            }

            var transaction = LedgerTransaction.Post(
                transactionId,
                command.Kind,
                amount.Value.Currency,
                [
                    new Posting(debited.Id, PostingDirection.Debit, amount.Value.MinorUnits),
                    new Posting(credited.Id, PostingDirection.Credit, amount.Value.MinorUnits),
                ],
                fingerprint,
                command.Reference,
                clock.GetUtcNow().UtcDateTime);
            if (transaction.IsFailure)
            {
                return transaction.Error;
            }

            session.Track(transaction.Value);
            session.Track(debited);
            session.Track(credited);
            await session.CommitAsync(ct);
            return Response(transaction.Value, command, replayed: false);
        }, cancellationToken);
    }

    private static TransactionResponse Response(LedgerTransaction transaction, MoveFundsCommand command, bool replayed) => new(
        transaction.Id,
        transaction.Kind,
        command.AccountId,
        Money.ToMajor(transaction.Postings[0].AmountMinor, transaction.Currency),
        transaction.Currency,
        transaction.PostedAtUtc,
        replayed);
}

internal static class AccountMapping
{
    public static AccountResponse ToResponse(this Account account) => new(
        account.Id,
        account.Holder,
        account.Currency,
        account.Kind,
        account.Status,
        Money.ToMajor(account.BalanceMinor, account.Currency),
        Money.ToMajor(account.HeldMinor, account.Currency),
        Money.ToMajor(account.AvailableMinor, account.Currency),
        account.Version);
}
