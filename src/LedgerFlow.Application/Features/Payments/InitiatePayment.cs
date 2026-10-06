using FluentValidation;
using LedgerFlow.Application.Abstractions;
using LedgerFlow.Application.Abstractions.Messaging;
using LedgerFlow.Application.Diagnostics;
using LedgerFlow.Application.Messaging;
using LedgerFlow.Domain.Accounts;
using LedgerFlow.Domain.Common;
using LedgerFlow.Domain.Payments;
using LedgerFlow.Domain.Values;
using Microsoft.EntityFrameworkCore;

namespace LedgerFlow.Application.Features.Payments;

public sealed record PaymentResponse(
    Guid PaymentId,
    PaymentStatus Status,
    Guid FromAccountId,
    Guid ToAccountId,
    decimal Amount,
    string Currency,
    string? Reference,
    int? RiskScore,
    string? FailureCode,
    string? FailureReason,
    DateTime InitiatedAtUtc,
    DateTime? FinishedAtUtc,
    bool Replayed = false);

/// <summary>
/// Accepts a payment and returns immediately (202). The payment id is derived from client + Idempotency-Key and
/// its stream is created with "no stream" as expected version: two concurrent retries of the same request cannot
/// both create a payment — the second one gets the first one's result.
/// </summary>
public sealed record InitiatePaymentCommand(
    string ClientId,
    string IdempotencyKey,
    Guid FromAccountId,
    Guid ToAccountId,
    decimal Amount,
    string Currency,
    string? Reference) : ICommand<PaymentResponse>;

internal sealed class InitiatePaymentValidator : AbstractValidator<InitiatePaymentCommand>
{
    public InitiatePaymentValidator()
    {
        RuleFor(c => c.IdempotencyKey).NotEmpty().MaximumLength(100);
        RuleFor(c => c.FromAccountId).NotEmpty();
        RuleFor(c => c.ToAccountId).NotEmpty().NotEqual(c => c.FromAccountId);
        RuleFor(c => c.Amount).GreaterThan(0);
        RuleFor(c => c.Currency).NotEmpty().Length(3);
        RuleFor(c => c.Reference).MaximumLength(Payment.ReferenceMaxLength);
    }
}

internal sealed class InitiatePaymentHandler(ILedgerSession session, ILedgerReadStore read, TimeProvider clock)
    : ICommandHandler<InitiatePaymentCommand, PaymentResponse>
{
    public async Task<Result<PaymentResponse>> Handle(InitiatePaymentCommand command, CancellationToken cancellationToken)
    {
        var amount = Money.FromMajor(command.Amount, command.Currency);
        if (amount.IsFailure)
        {
            return amount.Error;
        }

        var paymentId = Deterministic.Id("payment", command.ClientId, command.IdempotencyKey);
        var fingerprint = Deterministic.Fingerprint(
            command.FromAccountId, command.ToAccountId, amount.Value.MinorUnits, amount.Value.Currency.Code, command.Reference);

        var existing = await session.LoadAsync<Payment>(paymentId, cancellationToken);
        if (existing is not null)
        {
            return Replay(existing, fingerprint);
        }

        // Fail fast on obviously invalid requests (read model is updated in the same transaction as the events).
        var accounts = await read.Accounts
            .Where(a => a.AccountId == command.FromAccountId || a.AccountId == command.ToAccountId)
            .Select(a => new { a.AccountId, a.Currency, a.Kind })
            .ToListAsync(cancellationToken);

        foreach (var id in new[] { command.FromAccountId, command.ToAccountId })
        {
            var account = accounts.FirstOrDefault(a => a.AccountId == id);
            if (account is null || account.Kind != AccountKind.Customer)
            {
                return AccountErrors.NotFound(id);
            }

            if (account.Currency != amount.Value.Currency.Code)
            {
                return MoneyErrors.CurrencyMismatch(account.Currency, amount.Value.Currency.Code);
            }
        }

        var payment = Payment.Initiate(
            paymentId,
            command.ClientId,
            command.IdempotencyKey,
            fingerprint,
            command.FromAccountId,
            command.ToAccountId,
            amount.Value,
            command.Reference,
            clock.GetUtcNow().UtcDateTime);
        if (payment.IsFailure)
        {
            return payment.Error;
        }

        session.Track(payment.Value);
        session.Publish(new ReservePaymentFunds(paymentId, command.FromAccountId));

        try
        {
            await session.CommitAsync(cancellationToken);
        }
        catch (ConcurrencyConflictException)
        {
            // A concurrent retry with the same key won the race; answer with its result.
            session.Reset();
            var winner = await session.LoadAsync<Payment>(paymentId, cancellationToken);
            return winner is null ? throw new InvalidOperationException("Payment stream conflict without a stream.") : Replay(winner, fingerprint);
        }

        LedgerTelemetry.PaymentsInitiated.Add(1, new KeyValuePair<string, object?>("currency", amount.Value.Currency.Code));
        return payment.Value.ToResponse();
    }

    private static Result<PaymentResponse> Replay(Payment existing, string fingerprint) =>
        existing.RequestFingerprint == fingerprint
            ? existing.ToResponse() with { Replayed = true }
            : PaymentErrors.IdempotencyKeyReused;
}

public sealed record GetPaymentQuery(Guid PaymentId) : IQuery<PaymentResponse>;

internal sealed class GetPaymentHandler(ILedgerReadStore read) : IQueryHandler<GetPaymentQuery, PaymentResponse>
{
    public async Task<Result<PaymentResponse>> Handle(GetPaymentQuery query, CancellationToken cancellationToken)
    {
        var view = await read.Payments.FirstOrDefaultAsync(p => p.PaymentId == query.PaymentId, cancellationToken);
        if (view is null)
        {
            return PaymentErrors.NotFound(query.PaymentId);
        }

        return new PaymentResponse(
            view.PaymentId,
            view.Status,
            view.FromAccountId,
            view.ToAccountId,
            Money.ToMajor(view.AmountMinor, view.Currency),
            view.Currency,
            view.Reference,
            view.RiskScore,
            view.FailureCode,
            view.FailureReason,
            view.InitiatedAtUtc,
            view.FinishedAtUtc);
    }
}

internal static class PaymentMapping
{
    public static PaymentResponse ToResponse(this Payment payment) => new(
        payment.Id,
        payment.Status,
        payment.FromAccountId,
        payment.ToAccountId,
        Money.ToMajor(payment.AmountMinor, payment.Currency),
        payment.Currency,
        payment.Reference,
        payment.RiskScore,
        payment.FailureCode,
        payment.FailureReason,
        payment.InitiatedAtUtc,
        payment.FinishedAtUtc);
}
