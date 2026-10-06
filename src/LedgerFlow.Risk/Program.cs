using Grpc.Core;
using LedgerFlow.Contracts.Risk;
using LedgerFlow.Risk;
using OpenTelemetry;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddGrpc();
builder.Services.AddOptions<RiskRulesOptions>().Bind(builder.Configuration.GetSection(RiskRulesOptions.Section));
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<RiskRuleEngine>();

var redis = builder.Configuration.GetConnectionString("Redis");
if (string.IsNullOrWhiteSpace(redis))
{
    builder.Services.AddSingleton<IVelocityStore, InMemoryVelocityStore>();
}
else
{
    builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redis));
    builder.Services.AddSingleton<IVelocityStore, RedisVelocityStore>();
}

builder.Services.AddHealthChecks();
var otel = builder.Services.AddOpenTelemetry()
    .ConfigureResource(resource => resource.AddService(builder.Environment.ApplicationName))
    .WithTracing(tracing => tracing.AddAspNetCoreInstrumentation());
if (!string.IsNullOrWhiteSpace(builder.Configuration["OTEL_EXPORTER_OTLP_ENDPOINT"]))
{
    otel.UseOtlpExporter();
}

var app = builder.Build();
app.MapGrpcService<RiskEngineService>();
app.MapHealthChecks("/health/live");
await app.RunAsync();

internal sealed class RiskEngineService(RiskRuleEngine engine) : RiskEngine.RiskEngineBase
{
    public override async Task<AssessReply> Assess(AssessRequest request, ServerCallContext context)
    {
        if (!Guid.TryParse(request.PaymentId, out var paymentId) ||
            !Guid.TryParse(request.FromAccountId, out var from) ||
            !Guid.TryParse(request.ToAccountId, out var to) ||
            request.AmountMinor <= 0)
        {
            throw new RpcException(new Grpc.Core.Status(Grpc.Core.StatusCode.InvalidArgument, "payment_id, account ids and a positive amount are required."));
        }

        var verdict = await engine.EvaluateAsync(paymentId, from, to, request.AmountMinor, request.Currency);
        var reply = new AssessReply { Approved = verdict.Approved, Score = verdict.Score, Reason = verdict.Reason ?? string.Empty };
        reply.TriggeredRules.AddRange(verdict.TriggeredRules);
        return reply;
    }
}

/// <summary>Entry point marker for <c>WebApplicationFactory&lt;Program&gt;</c>.</summary>
public partial class Program;
