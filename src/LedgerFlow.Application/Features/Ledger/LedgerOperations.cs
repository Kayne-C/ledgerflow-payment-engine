using FluentValidation;
using LedgerFlow.Application.Abstractions;
using LedgerFlow.Application.Abstractions.Messaging;
using LedgerFlow.Application.Messaging;
using LedgerFlow.Domain.Accounts;
using LedgerFlow.Domain.Common;
using LedgerFlow.Domain.Payments;
using LedgerFlow.Domain.Transactions;
using LedgerFlow.Domain.Values;
using Microsoft.EntityFrameworkCore;

namespace LedgerFlow.Application.Features.Ledger;

public sealed record CurrencyBalance(string Currency, decimal CustomerTotal, decimal ClearingTotal, decimal Net);

public sealed record TrialBalanceResponse(bool IsBalanced, IReadOnlyList<CurrencyBalance> Currencies);

/// <summary>Double-entry check: per currency, customer balances and clearing balances must cancel out exactly.</summary>
public sealed record GetTrialBalanceQuery : IQuery<TrialBalanceResponse>;

internal sealed class GetTrialBalanceHandler(ILedgerReadStore read) : IQueryHandler<GetTrialBalanceQuery, TrialBalanceResponse>
{
    public async Task<Result<TrialBalanceResponse>> Handle(GetTrialBalanceQuery query, CancellationToken cancellationToken)
    {
        var totals = await read.Accounts
            .GroupBy(a => new { a.Currency, a.Kind })
            .Select(g => new { g.Key.Currency, g.Key.Kind, Total = g.Sum(a => a.BalanceMinor) })
            .ToListAsync(cancellationToken);

        var currencies = totals
            .GroupBy(t => t.Currency)
            .OrderBy(g => g.Key)
            .Select(g =>
            {
                var customer = g.Where(t => t.Kind == AccountKind.Customer).Sum(t => t.Total);
                var clearing = g.Where(t => t.Kind == AccountKind.Clearing).Sum(t => t.Total);
                return new CurrencyBalance(g.Key, Money.ToMajor(customer, g.Key), Money.ToMajor(clearing, g.Key), Money.ToMajor(customer + clearing, g.Key));
            })
            .ToList();

        return new TrialBalanceResponse(currencies.All(c => c.Net == 0), currencies);
    }
}

public sealed record ReconciliationIssue(string Kind, string Subject, string Detail);

public sealed record ReconciliationResponse(
    bool IsClean,
    int AccountsChecked,
    long LedgerEntriesChecked,
    int PaymentsInFlight,
    IReadOnlyList<ReconciliationIssue> Issues);

/// <summary>
/// Independent consistency audit used after load and chaos tests:
/// trial balance is zero, every account balance equals the sum of its journal lines, no customer is overdrawn,
/// and every held amount belongs to a payment that is still in flight.
/// </summary>
public sealed record ReconcileLedgerQuery : IQuery<ReconciliationResponse>;

internal sealed class ReconcileLedgerHandler(ILedgerReadStore read, ISender sender) : IQueryHandler<ReconcileLedgerQuery, ReconciliationResponse>
{
    public async Task<Result<ReconciliationResponse>> Handle(ReconcileLedgerQuery query, CancellationToken cancellationToken)
    {
        var issues = new List<ReconciliationIssue>();

        var trial = await sender.Send(new GetTrialBalanceQuery(), cancellationToken);
        issues.AddRange(trial.Value.Currencies.Where(c => c.Net != 0)
            .Select(c => new ReconciliationIssue("TrialBalance", c.Currency, $"Net is {c.Net}, expected 0.")));

        var accounts = await read.Accounts
            .Select(a => new { a.AccountId, a.Kind, a.BalanceMinor, a.HeldMinor })
            .ToListAsync(cancellationToken);

        var journal = await read.LedgerEntries
            .GroupBy(e => new { e.AccountId, e.Direction })
            .Select(g => new { g.Key.AccountId, g.Key.Direction, Total = g.Sum(e => e.AmountMinor), Count = g.Count() })
            .ToListAsync(cancellationToken);

        var heldByPayments = await read.Payments
            .Where(p => p.Status == PaymentStatus.FundsReserved || p.Status == PaymentStatus.RiskApproved || p.Status == PaymentStatus.RiskRejected)
            .GroupBy(p => p.FromAccountId)
            .Select(g => new { AccountId = g.Key, Total = g.Sum(p => p.AmountMinor) })
            .ToDictionaryAsync(x => x.AccountId, x => x.Total, cancellationToken);

        foreach (var account in accounts)
        {
            var credits = journal.Where(j => j.AccountId == account.AccountId && j.Direction == PostingDirection.Credit).Sum(j => j.Total);
            var debits = journal.Where(j => j.AccountId == account.AccountId && j.Direction == PostingDirection.Debit).Sum(j => j.Total);
            if (credits - debits != account.BalanceMinor)
            {
                issues.Add(new ReconciliationIssue("Balance", account.AccountId.ToString(), $"Balance {account.BalanceMinor} ≠ journal {credits - debits}."));
            }

            if (account.Kind == AccountKind.Customer && account.BalanceMinor - account.HeldMinor < 0)
            {
                issues.Add(new ReconciliationIssue("Overdraft", account.AccountId.ToString(), $"Available {account.BalanceMinor - account.HeldMinor} < 0."));
            }

            var expectedHeld = heldByPayments.GetValueOrDefault(account.AccountId);
            if (account.HeldMinor != expectedHeld)
            {
                issues.Add(new ReconciliationIssue("Holds", account.AccountId.ToString(), $"Held {account.HeldMinor} ≠ in-flight payments {expectedHeld}."));
            }
        }

        var inFlight = await read.Payments.CountAsync(p => p.Status != PaymentStatus.Completed && p.Status != PaymentStatus.Failed, cancellationToken);
        return new ReconciliationResponse(issues.Count == 0, accounts.Count, journal.Sum(j => (long)j.Count), inFlight, issues);
    }
}

public sealed record VerifyStreamIntegrityQuery(string StreamId) : IQuery<IntegrityReport>;

internal sealed class VerifyStreamIntegrityValidator : AbstractValidator<VerifyStreamIntegrityQuery>
{
    public VerifyStreamIntegrityValidator() => RuleFor(q => q.StreamId).NotEmpty().MaximumLength(100);
}

internal sealed class VerifyStreamIntegrityHandler(IEventStoreAudit audit) : IQueryHandler<VerifyStreamIntegrityQuery, IntegrityReport>
{
    public async Task<Result<IntegrityReport>> Handle(VerifyStreamIntegrityQuery query, CancellationToken cancellationToken)
    {
        var report = await audit.VerifyStreamAsync(query.StreamId, cancellationToken);
        return report.EventCount == 0 ? Error.NotFound("Stream.NotFound", $"Stream '{query.StreamId}' does not exist.") : report;
    }
}

public sealed record LedgerStatsResponse(
    IReadOnlyDictionary<string, int> PaymentsByStatus,
    long Events,
    long PendingOutbox,
    DateTime? FirstPaymentUtc,
    DateTime? LastFinishedUtc);

public sealed record GetLedgerStatsQuery : IQuery<LedgerStatsResponse>;

internal sealed class GetLedgerStatsHandler(ILedgerReadStore read, IEventStoreAudit audit) : IQueryHandler<GetLedgerStatsQuery, LedgerStatsResponse>
{
    public async Task<Result<LedgerStatsResponse>> Handle(GetLedgerStatsQuery query, CancellationToken cancellationToken)
    {
        var byStatus = await read.Payments.GroupBy(p => p.Status).Select(g => new { g.Key, Count = g.Count() }).ToListAsync(cancellationToken);
        var first = await read.Payments.MinAsync(p => (DateTime?)p.InitiatedAtUtc, cancellationToken);
        var last = await read.Payments.MaxAsync(p => p.FinishedAtUtc, cancellationToken);

        return new LedgerStatsResponse(
            byStatus.ToDictionary(s => s.Key.ToString(), s => s.Count),
            await audit.CountEventsAsync(cancellationToken),
            await audit.CountPendingOutboxAsync(cancellationToken),
            first,
            last);
    }
}

public sealed record RedriveResponse(int Redriven, int TimedOut);

/// <summary>
/// Safety net for at-least-once pipelines: re-publishes the next step of payments that stopped moving (e.g. their
/// message went to the DLQ during an outage) and compensates payments that exceeded the hard timeout.
/// </summary>
public sealed record RedriveStalledPaymentsCommand(TimeSpan StalledFor, TimeSpan HardTimeout, int BatchSize = 500) : ICommand<RedriveResponse>;

internal sealed class RedriveStalledPaymentsHandler(ILedgerReadStore read, ILedgerSession session, TimeProvider clock)
    : ICommandHandler<RedriveStalledPaymentsCommand, RedriveResponse>
{
    public async Task<Result<RedriveResponse>> Handle(RedriveStalledPaymentsCommand command, CancellationToken cancellationToken)
    {
        var now = clock.GetUtcNow().UtcDateTime;
        var stalledBefore = now - command.StalledFor;
        var stalled = await read.Payments
            .Where(p => p.Status != PaymentStatus.Completed && p.Status != PaymentStatus.Failed && p.UpdatedAtUtc < stalledBefore)
            .OrderBy(p => p.UpdatedAtUtc)
            .Take(command.BatchSize)
            .Select(p => new { p.PaymentId, p.FromAccountId, p.Status, p.InitiatedAtUtc })
            .ToListAsync(cancellationToken);

        var timedOut = 0;
        foreach (var payment in stalled)
        {
            if (now - payment.InitiatedAtUtc > command.HardTimeout)
            {
                session.Publish(new ReleasePaymentFunds(payment.PaymentId, payment.FromAccountId, "Payment.Timeout", "The payment did not finish in time."));
                timedOut++;
                continue;
            }

            session.Publish(payment.Status switch
            {
                PaymentStatus.Initiated => new ReservePaymentFunds(payment.PaymentId, payment.FromAccountId),
                PaymentStatus.FundsReserved => new AssessPaymentRisk(payment.PaymentId),
                PaymentStatus.RiskApproved => new SettlePayment(payment.PaymentId, payment.FromAccountId),
                _ => new ReleasePaymentFunds(payment.PaymentId, payment.FromAccountId, "Risk.Rejected", "Rejected by risk engine."),
            });
        }

        if (stalled.Count > 0)
        {
            await session.CommitAsync(cancellationToken);
        }

        return new RedriveResponse(stalled.Count - timedOut, timedOut);
    }
}
