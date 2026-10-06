using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace LedgerFlow.Application.Diagnostics;

public static class LedgerTelemetry
{
    public const string SourceName = "LedgerFlow";

    public static readonly ActivitySource ActivitySource = new(SourceName);

    private static readonly Meter Meter = new(SourceName);

    public static readonly Histogram<double> RequestDuration =
        Meter.CreateHistogram<double>("ledgerflow.request.duration", "ms", "Application use case duration.");

    public static readonly Counter<long> PaymentsInitiated =
        Meter.CreateCounter<long>("ledgerflow.payments.initiated", description: "Payments accepted by the API.");

    public static readonly Counter<long> PaymentsFinished =
        Meter.CreateCounter<long>("ledgerflow.payments.finished", description: "Payments that reached a terminal state.");

    public static readonly Counter<long> ConcurrencyConflicts =
        Meter.CreateCounter<long>("ledgerflow.eventstore.conflicts", description: "Appends rejected by an expected-version check and retried.");

    public static readonly Counter<long> OutboxPublished =
        Meter.CreateCounter<long>("ledgerflow.outbox.published", description: "Outbox messages delivered to the broker.");
}
