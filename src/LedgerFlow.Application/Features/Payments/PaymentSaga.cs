using LedgerFlow.Application.Abstractions;
using LedgerFlow.Application.Abstractions.Messaging;
using LedgerFlow.Application.Common;
using LedgerFlow.Application.Diagnostics;
using LedgerFlow.Application.Messaging;
using LedgerFlow.Domain.Accounts;
using LedgerFlow.Domain.Common;
using LedgerFlow.Domain.Payments;
using LedgerFlow.Domain.Transactions;
using LedgerFlow.Domain.Values;

namespace LedgerFlow.Application.Features.Payments;

public enum SagaStepOutcome
{
    Advanced,
    Failed,

    /// <summary>Redelivered or out-of-date message: the payment is already past this step.</summary>
    AlreadyHandled,
}

/// <summary>
/// Orchestrated payment saga. Every step is: load → decide → append (with expected versions) → enqueue the next
/// step in the same transaction. Steps are idempotent because the payment's state machine rejects a step that
/// has already happened, and concurrent duplicates lose the expected-version race.
/// </summary>
public sealed record ReservePaymentFundsCommand(Guid PaymentId) : ICommand<SagaStepOutcome>;

internal sealed class ReservePaymentFundsHandler(ILedgerSession session, TimeProvider clock)
    : ICommandHandler<ReservePaymentFundsCommand, SagaStepOutcome>
{
    public Task<Result<SagaStepOutcome>> Handle(ReservePaymentFundsCommand command, CancellationToken cancellationToken) =>
        Optimistic.RetryAsync<Result<SagaStepOutcome>>(session, async ct =>
        {
            var payment = await session.LoadAsync<Payment>(command.PaymentId, ct);
            if (payment is null)
            {
                return PaymentErrors.NotFound(command.PaymentId);
            }

            if (payment.Status != PaymentStatus.Initiated)
            {
                return SagaStepOutcome.AlreadyHandled;
            }

            var source = await session.LoadAsync<Account>(payment.FromAccountId, ct);
            var hold = source is null ? AccountErrors.NotFound(payment.FromAccountId) : source.PlaceHold(payment.Id, payment.Amount);

            if (hold.IsFailure)
            {
                payment.Fail(hold.Error.Code, hold.Error.Description, clock.GetUtcNow().UtcDateTime);
                session.Publish(new PaymentRejected(payment.Id, payment.FromAccountId, hold.Error.Code, hold.Error.Description));
                session.Track(payment);
                await session.CommitAsync(ct);
                PaymentSagaMetrics.Finished(payment);
                return SagaStepOutcome.Failed;
            }

            payment.MarkFundsReserved();
            session.Track(source!);
            session.Track(payment);
            session.Publish(new AssessPaymentRisk(payment.Id));
            await session.CommitAsync(ct);
            return SagaStepOutcome.Advanced;
        }, cancellationToken);
}

public sealed record AssessPaymentRiskCommand(Guid PaymentId) : ICommand<SagaStepOutcome>;

internal sealed class AssessPaymentRiskHandler(ILedgerSession session, IRiskAssessor risk)
    : ICommandHandler<AssessPaymentRiskCommand, SagaStepOutcome>
{
    public Task<Result<SagaStepOutcome>> Handle(AssessPaymentRiskCommand command, CancellationToken cancellationToken) =>
        Optimistic.RetryAsync<Result<SagaStepOutcome>>(session, async ct =>
        {
            var payment = await session.LoadAsync<Payment>(command.PaymentId, ct);
            if (payment is null)
            {
                return PaymentErrors.NotFound(command.PaymentId);
            }

            if (payment.Status != PaymentStatus.FundsReserved)
            {
                return SagaStepOutcome.AlreadyHandled;
            }

            // The external decision is recorded as an event, so replays never call the risk engine again.
            var decision = await risk.AssessAsync(
                new RiskRequest(payment.Id, payment.FromAccountId, payment.ToAccountId, payment.AmountMinor, payment.Currency), ct);

            payment.RecordRiskDecision(decision.Approved, decision.Score, decision.Reason);
            session.Track(payment);
            session.Publish(decision.Approved
                ? new SettlePayment(payment.Id, payment.FromAccountId)
                : new ReleasePaymentFunds(payment.Id, payment.FromAccountId, "Risk.Rejected", decision.Reason ?? "Rejected by risk engine."));
            await session.CommitAsync(ct);
            return decision.Approved ? SagaStepOutcome.Advanced : SagaStepOutcome.Failed;
        }, cancellationToken);
}

public sealed record SettlePaymentCommand(Guid PaymentId) : ICommand<SagaStepOutcome>;

/// <summary>
/// Final step: one atomic append across four streams — the journal entry (created with "no stream", so a payment
/// can settle only once), the debit that captures the hold, the credit, and the payment's completion.
/// </summary>
internal sealed class SettlePaymentHandler(ILedgerSession session, TimeProvider clock)
    : ICommandHandler<SettlePaymentCommand, SagaStepOutcome>
{
    public Task<Result<SagaStepOutcome>> Handle(SettlePaymentCommand command, CancellationToken cancellationToken) =>
        Optimistic.RetryAsync<Result<SagaStepOutcome>>(session, async ct =>
        {
            var payment = await session.LoadAsync<Payment>(command.PaymentId, ct);
            if (payment is null)
            {
                return PaymentErrors.NotFound(command.PaymentId);
            }

            if (payment.Status != PaymentStatus.RiskApproved)
            {
                return SagaStepOutcome.AlreadyHandled;
            }

            var utcNow = clock.GetUtcNow().UtcDateTime;
            var source = await session.LoadAsync<Account>(payment.FromAccountId, ct);
            var destination = await session.LoadAsync<Account>(payment.ToAccountId, ct);
            var transactionId = payment.Id;

            var capture = source?.CaptureHold(payment.Id, transactionId) ?? AccountErrors.NotFound(payment.FromAccountId);
            var credit = capture.IsFailure
                ? capture
                : destination?.Credit(transactionId, payment.Amount) ?? AccountErrors.NotFound(payment.ToAccountId);

            if (credit.IsFailure)
            {
                // Compensate inside the same unit of work: drop the capture, release the hold, fail the payment.
                session.Reset();
                return await CompensateAsync(command.PaymentId, credit.Error, ct);
            }

            var journal = LedgerTransaction.Post(
                transactionId,
                TransactionKind.Payment,
                payment.Amount.Currency,
                [
                    new Posting(payment.FromAccountId, PostingDirection.Debit, payment.AmountMinor),
                    new Posting(payment.ToAccountId, PostingDirection.Credit, payment.AmountMinor),
                ],
                payment.RequestFingerprint,
                payment.Reference,
                utcNow).Value;

            payment.Complete(transactionId, utcNow);
            session.Track(journal);
            session.Track(source!);
            session.Track(destination!);
            session.Track(payment);
            session.Publish(new PaymentSucceeded(payment.Id, transactionId, payment.FromAccountId, payment.ToAccountId, payment.AmountMinor, payment.Currency));
            await session.CommitAsync(ct);
            PaymentSagaMetrics.Finished(payment);
            return SagaStepOutcome.Advanced;
        }, cancellationToken);

    private async Task<Result<SagaStepOutcome>> CompensateAsync(Guid paymentId, Error reason, CancellationToken ct)
    {
        var payment = (await session.LoadAsync<Payment>(paymentId, ct))!;
        var source = await session.LoadAsync<Account>(payment.FromAccountId, ct);
        source?.ReleaseHold(payment.Id);
        payment.Fail(reason.Code, reason.Description, clock.GetUtcNow().UtcDateTime);

        if (source is not null)
        {
            session.Track(source);
        }

        session.Track(payment);
        session.Publish(new PaymentRejected(payment.Id, payment.FromAccountId, reason.Code, reason.Description));
        await session.CommitAsync(ct);
        PaymentSagaMetrics.Finished(payment);
        return SagaStepOutcome.Failed;
    }
}

public sealed record ReleasePaymentFundsCommand(Guid PaymentId, string ReasonCode, string Reason) : ICommand<SagaStepOutcome>;

/// <summary>Compensation: release the hold and fail the payment. Safe to run for a payment in any non-terminal state.</summary>
internal sealed class ReleasePaymentFundsHandler(ILedgerSession session, TimeProvider clock)
    : ICommandHandler<ReleasePaymentFundsCommand, SagaStepOutcome>
{
    public Task<Result<SagaStepOutcome>> Handle(ReleasePaymentFundsCommand command, CancellationToken cancellationToken) =>
        Optimistic.RetryAsync<Result<SagaStepOutcome>>(session, async ct =>
        {
            var payment = await session.LoadAsync<Payment>(command.PaymentId, ct);
            if (payment is null)
            {
                return PaymentErrors.NotFound(command.PaymentId);
            }

            if (payment.IsTerminal)
            {
                return SagaStepOutcome.AlreadyHandled;
            }

            var source = await session.LoadAsync<Account>(payment.FromAccountId, ct);
            if (source is not null)
            {
                source.ReleaseHold(payment.Id);
                session.Track(source);
            }

            payment.Fail(command.ReasonCode, command.Reason, clock.GetUtcNow().UtcDateTime);
            session.Track(payment);
            session.Publish(new PaymentRejected(payment.Id, payment.FromAccountId, command.ReasonCode, command.Reason));
            await session.CommitAsync(ct);
            PaymentSagaMetrics.Finished(payment);
            return SagaStepOutcome.Failed;
        }, cancellationToken);
}

internal static class PaymentSagaMetrics
{
    public static void Finished(Payment payment) => LedgerTelemetry.PaymentsFinished.Add(
        1,
        new KeyValuePair<string, object?>("status", payment.Status.ToString()),
        new KeyValuePair<string, object?>("reason", payment.FailureCode ?? "none"));
}

/// <summary>Broker adapters: translate saga messages into commands so they run through the same pipeline (validation, telemetry).</summary>
internal sealed class PaymentSagaMessageHandlers(ISender sender) :
    IMessageHandler<ReservePaymentFunds>,
    IMessageHandler<AssessPaymentRisk>,
    IMessageHandler<SettlePayment>,
    IMessageHandler<ReleasePaymentFunds>
{
    public Task HandleAsync(ReservePaymentFunds message, CancellationToken cancellationToken) =>
        Run(new ReservePaymentFundsCommand(message.PaymentId), cancellationToken);

    public Task HandleAsync(AssessPaymentRisk message, CancellationToken cancellationToken) =>
        Run(new AssessPaymentRiskCommand(message.PaymentId), cancellationToken);

    public Task HandleAsync(SettlePayment message, CancellationToken cancellationToken) =>
        Run(new SettlePaymentCommand(message.PaymentId), cancellationToken);

    public Task HandleAsync(ReleasePaymentFunds message, CancellationToken cancellationToken) =>
        Run(new ReleasePaymentFundsCommand(message.PaymentId, message.ReasonCode, message.Reason), cancellationToken);

    private async Task Run(ICommand<SagaStepOutcome> command, CancellationToken cancellationToken)
    {
        var result = await sender.Send(command, cancellationToken);

        // Unknown payments are dropped (logged by the telemetry behavior); anything else unexpected is retried by the consumer.
        if (result.IsFailure && result.Error.Type != ErrorType.NotFound)
        {
            throw new InvalidOperationException($"Saga step {command.GetType().Name} failed: {result.Error.Code}");
        }
    }
}
