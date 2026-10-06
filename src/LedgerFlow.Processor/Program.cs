using LedgerFlow.Application;
using LedgerFlow.Application.Diagnostics;
using LedgerFlow.Infrastructure;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

// Saga processor: relays the outbox to Kafka, consumes saga commands (partitioned by source account),
// calls the risk engine over gRPC and re-drives stalled payments. Scale out by adding replicas.
var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddApplication()
    .AddInfrastructure(builder.Configuration)
    .AddLedgerBackgroundServices();

var otel = builder.Services.AddOpenTelemetry()
    .ConfigureResource(resource => resource.AddService(builder.Environment.ApplicationName))
    .WithTracing(tracing => tracing.AddSource(LedgerTelemetry.SourceName).AddHttpClientInstrumentation())
    .WithMetrics(metrics => metrics.AddMeter(LedgerTelemetry.SourceName).AddRuntimeInstrumentation());

builder.Logging.AddOpenTelemetry(logging => logging.IncludeFormattedMessage = true);
if (!string.IsNullOrWhiteSpace(builder.Configuration["OTEL_EXPORTER_OTLP_ENDPOINT"]))
{
    otel.UseOtlpExporter();
}

await builder.Build().RunAsync();
