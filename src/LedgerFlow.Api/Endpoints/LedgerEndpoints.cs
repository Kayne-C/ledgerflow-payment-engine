using LedgerFlow.Api.Infrastructure;
using LedgerFlow.Api.Security;
using LedgerFlow.Application.Abstractions;
using LedgerFlow.Application.Abstractions.Messaging;
using LedgerFlow.Application.Common;
using LedgerFlow.Application.Features.Accounts;
using LedgerFlow.Application.Features.Ledger;
using LedgerFlow.Application.Features.Payments;
using LedgerFlow.Domain.Transactions;

namespace LedgerFlow.Api.Endpoints;

public sealed record OpenAccountRequest(string Holder, string Currency);

public sealed record MoveFundsRequest(decimal Amount, string Currency, string? Reference);

public sealed record CreatePaymentRequest(Guid FromAccountId, Guid ToAccountId, decimal Amount, string Currency, string? Reference);

public sealed record FreezeAccountRequest(string Reason);

internal static class LedgerEndpoints
{
    public static RouteGroupBuilder MapAccountEndpoints(this RouteGroupBuilder api)
    {
        var accounts = api.MapGroup("/accounts").WithTags("Accounts").RequireAuthorization(ApiScopes.Payments);

        accounts.MapPost("/", async (OpenAccountRequest request, ISender sender, CancellationToken ct) =>
                (await sender.Send(new OpenAccountCommand(request.Holder, request.Currency), ct))
                .ToHttp(account => TypedResults.Created($"/api/v1/accounts/{account.AccountId}", account)))
            .Produces<AccountResponse>(StatusCodes.Status201Created)
            .ProducesValidationProblem();

        accounts.MapGet("/{accountId:guid}", async (Guid accountId, ISender sender, CancellationToken ct) =>
                (await sender.Send(new GetAccountQuery(accountId), ct)).ToHttp(TypedResults.Ok))
            .Produces<AccountResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        accounts.MapGet("/{accountId:guid}/statement", async (
                    Guid accountId, ISender sender, CancellationToken ct, DateTime? from = null, DateTime? to = null, int page = 1, int pageSize = 50) =>
                (await sender.Send(new GetStatementQuery(accountId, from, to, page, pageSize), ct)).ToHttp(TypedResults.Ok))
            .WithSummary("Journal lines of the account (newest first).")
            .Produces<PagedResponse<StatementLine>>();

        accounts.MapGet("/{accountId:guid}/balance-at", async (Guid accountId, DateTime at, ISender sender, CancellationToken ct) =>
                (await sender.Send(new GetBalanceAtQuery(accountId, at.ToUniversalTime()), ct)).ToHttp(TypedResults.Ok))
            .WithSummary("Temporal query: balance as of a past instant, rebuilt from snapshots + events.")
            .Produces<BalanceAtResponse>();

        accounts.MapPost("/{accountId:guid}/deposits", (Guid accountId, MoveFundsRequest request, HttpContext http, ISender sender, CancellationToken ct) =>
                MoveFundsAsync(TransactionKind.Deposit, accountId, request, http, sender, ct))
            .WithSummary("Credit the account from the clearing account (idempotent).")
            .Produces<TransactionResponse>(StatusCodes.Status201Created);

        accounts.MapPost("/{accountId:guid}/withdrawals", (Guid accountId, MoveFundsRequest request, HttpContext http, ISender sender, CancellationToken ct) =>
                MoveFundsAsync(TransactionKind.Withdrawal, accountId, request, http, sender, ct))
            .WithSummary("Debit the account to the clearing account (idempotent, never overdraws).")
            .Produces<TransactionResponse>(StatusCodes.Status201Created);

        return api;
    }

    public static RouteGroupBuilder MapPaymentEndpoints(this RouteGroupBuilder api)
    {
        var payments = api.MapGroup("/payments").WithTags("Payments").RequireAuthorization(ApiScopes.Payments);

        payments.MapPost("/", async (CreatePaymentRequest request, HttpContext http, ISender sender, CancellationToken ct) =>
            {
                if (!http.Request.TryGetIdempotencyKey(out var key, out var problem))
                {
                    return problem!;
                }

                var command = new InitiatePaymentCommand(
                    http.ClientId(), key, request.FromAccountId, request.ToAccountId, request.Amount, request.Currency, request.Reference);

                return (await sender.Send(command, ct)).ToHttp(payment =>
                {
                    http.MarkReplayed(payment.Replayed);
                    return TypedResults.Accepted($"/api/v1/payments/{payment.PaymentId}", payment);
                });
            })
            .WithSummary("Start a transfer. Returns 202; the saga reserves funds, checks risk and settles asynchronously.")
            .Produces<PaymentResponse>(StatusCodes.Status202Accepted)
            .ProducesValidationProblem()
            .ProducesProblem(StatusCodes.Status422UnprocessableEntity);

        payments.MapGet("/{paymentId:guid}", async (Guid paymentId, ISender sender, CancellationToken ct) =>
                (await sender.Send(new GetPaymentQuery(paymentId), ct)).ToHttp(TypedResults.Ok))
            .Produces<PaymentResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        return api;
    }

    public static RouteGroupBuilder MapAdminEndpoints(this RouteGroupBuilder api)
    {
        var admin = api.MapGroup("/admin").WithTags("Operations").RequireAuthorization(ApiScopes.Admin);

        admin.MapGet("/trial-balance", async (ISender sender, CancellationToken ct) =>
                (await sender.Send(new GetTrialBalanceQuery(), ct)).ToHttp(TypedResults.Ok))
            .Produces<TrialBalanceResponse>();

        admin.MapGet("/reconciliation", async (ISender sender, CancellationToken ct) =>
                (await sender.Send(new ReconcileLedgerQuery(), ct)).ToHttp(TypedResults.Ok))
            .WithSummary("Full consistency audit: trial balance, balances vs. journal, overdrafts, holds vs. in-flight payments.")
            .Produces<ReconciliationResponse>();

        admin.MapGet("/streams/{streamId}/integrity", async (string streamId, ISender sender, CancellationToken ct) =>
                (await sender.Send(new VerifyStreamIntegrityQuery(streamId), ct)).ToHttp(TypedResults.Ok))
            .WithSummary("Recompute the stream's hash chain to detect tampering.")
            .Produces<IntegrityReport>();

        admin.MapGet("/stats", async (ISender sender, CancellationToken ct) =>
                (await sender.Send(new GetLedgerStatsQuery(), ct)).ToHttp(TypedResults.Ok))
            .Produces<LedgerStatsResponse>();

        admin.MapPost("/accounts/{accountId:guid}/freeze", async (Guid accountId, FreezeAccountRequest request, ISender sender, CancellationToken ct) =>
                (await sender.Send(new FreezeAccountCommand(accountId, request.Reason), ct)).ToHttp(TypedResults.Ok))
            .Produces<AccountResponse>();

        return api;
    }

    private static async Task<IResult> MoveFundsAsync(
        TransactionKind kind, Guid accountId, MoveFundsRequest request, HttpContext http, ISender sender, CancellationToken ct)
    {
        if (!http.Request.TryGetIdempotencyKey(out var key, out var problem))
        {
            return problem!;
        }

        var command = new MoveFundsCommand(kind, http.ClientId(), key, accountId, request.Amount, request.Currency, request.Reference);
        return (await sender.Send(command, ct)).ToHttp(transaction =>
        {
            http.MarkReplayed(transaction.Replayed);
            return transaction.Replayed
                ? TypedResults.Ok(transaction)
                : TypedResults.Created($"/api/v1/accounts/{accountId}/statement", transaction);
        });
    }
}
