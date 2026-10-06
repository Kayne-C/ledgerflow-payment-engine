using FluentValidation;
using LedgerFlow.Application.Abstractions;
using LedgerFlow.Application.Abstractions.Messaging;
using LedgerFlow.Application.Common;
using LedgerFlow.Domain.Accounts;
using LedgerFlow.Domain.Common;
using LedgerFlow.Domain.Transactions;
using LedgerFlow.Domain.Values;
using Microsoft.EntityFrameworkCore;

namespace LedgerFlow.Application.Features.Accounts;

public sealed record GetAccountQuery(Guid AccountId) : IQuery<AccountResponse>;

internal sealed class GetAccountHandler(ILedgerReadStore read) : IQueryHandler<GetAccountQuery, AccountResponse>
{
    public async Task<Result<AccountResponse>> Handle(GetAccountQuery query, CancellationToken cancellationToken)
    {
        var view = await read.Accounts.FirstOrDefaultAsync(a => a.AccountId == query.AccountId, cancellationToken);
        if (view is null)
        {
            return AccountErrors.NotFound(query.AccountId);
        }

        return new AccountResponse(
            view.AccountId,
            view.Holder,
            view.Currency,
            view.Kind,
            view.Status,
            Money.ToMajor(view.BalanceMinor, view.Currency),
            Money.ToMajor(view.HeldMinor, view.Currency),
            Money.ToMajor(view.BalanceMinor - view.HeldMinor, view.Currency),
            view.Version);
    }
}

public sealed record StatementLine(Guid TransactionId, PostingDirection Direction, decimal Amount, decimal BalanceAfter, DateTime PostedAtUtc);

public sealed record GetStatementQuery(Guid AccountId, DateTime? FromUtc, DateTime? ToUtc, int Page = 1, int PageSize = 50)
    : IQuery<PagedResponse<StatementLine>>;

internal sealed class GetStatementValidator : AbstractValidator<GetStatementQuery>
{
    public GetStatementValidator()
    {
        RuleFor(q => q.Page).GreaterThanOrEqualTo(1);
        RuleFor(q => q.PageSize).InclusiveBetween(1, Paging.MaxPageSize);
    }
}

internal sealed class GetStatementHandler(ILedgerReadStore read) : IQueryHandler<GetStatementQuery, PagedResponse<StatementLine>>
{
    public async Task<Result<PagedResponse<StatementLine>>> Handle(GetStatementQuery query, CancellationToken cancellationToken)
    {
        var currency = await read.Accounts.Where(a => a.AccountId == query.AccountId).Select(a => a.Currency).FirstOrDefaultAsync(cancellationToken);
        if (currency is null)
        {
            return AccountErrors.NotFound(query.AccountId);
        }

        var entries = read.LedgerEntries.Where(e => e.AccountId == query.AccountId);
        if (query.FromUtc is { } from)
        {
            entries = entries.Where(e => e.PostedAtUtc >= from);
        }

        if (query.ToUtc is { } to)
        {
            entries = entries.Where(e => e.PostedAtUtc <= to);
        }

        var total = await entries.CountAsync(cancellationToken);
        var page = await entries
            .OrderByDescending(e => e.Id)
            .Skip((query.Page - 1) * query.PageSize)
            .Take(query.PageSize)
            .ToListAsync(cancellationToken);

        var lines = page
            .Select(e => new StatementLine(e.TransactionId, e.Direction, Money.ToMajor(e.AmountMinor, currency), Money.ToMajor(e.BalanceAfterMinor, currency), e.PostedAtUtc))
            .ToList();

        return new PagedResponse<StatementLine>(lines, query.Page, query.PageSize, total);
    }
}

public sealed record BalanceAtResponse(Guid AccountId, DateTime AsOfUtc, decimal Balance, decimal Held, string Currency, long Version);

/// <summary>"What was the balance at 23:59 on the last day of the quarter?" — answered from the event log, not a report table.</summary>
public sealed record GetBalanceAtQuery(Guid AccountId, DateTime AsOfUtc) : IQuery<BalanceAtResponse>;

internal sealed class GetBalanceAtHandler(ILedgerSession session) : IQueryHandler<GetBalanceAtQuery, BalanceAtResponse>
{
    public async Task<Result<BalanceAtResponse>> Handle(GetBalanceAtQuery query, CancellationToken cancellationToken)
    {
        var account = await session.LoadAsOfAsync<Account>(query.AccountId, query.AsOfUtc, cancellationToken);
        if (account is null)
        {
            return AccountErrors.NotFound(query.AccountId);
        }

        return new BalanceAtResponse(
            account.Id,
            query.AsOfUtc,
            Money.ToMajor(account.BalanceMinor, account.Currency),
            Money.ToMajor(account.HeldMinor, account.Currency),
            account.Currency,
            account.Version);
    }
}
