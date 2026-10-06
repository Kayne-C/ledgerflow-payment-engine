using System.Text.Json.Serialization;
using LedgerFlow.Api.Endpoints;
using LedgerFlow.Api.Infrastructure;
using LedgerFlow.Application;
using LedgerFlow.Infrastructure;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Scalar.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services
    .AddApplication()
    .AddInfrastructure(builder.Configuration)
    .AddLedgerBackgroundServices()
    .AddApiKeySecurity(builder.Configuration)
    .AddClientRateLimiting(builder.Configuration)
    .AddApiDocumentation()
    .AddProblemDetails()
    .AddExceptionHandler<GlobalExceptionHandler>()
    .ConfigureHttpJsonOptions(options => options.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));

builder.Services.AddHealthChecks().AddInfrastructureHealthChecks();
builder.AddObservability();

var app = builder.Build();

await app.Services.InitializeDatabaseAsync();

// One-shot mode for deployment pipelines / docker-compose: migrate (+ optionally seed) and exit.
if (args.Contains("--migrate-only"))
{
    return;
}

app.UseExceptionHandler();
app.UseStatusCodePages();
app.UseAuthentication();
app.UseRateLimiter();
app.UseAuthorization();

if (!app.Environment.IsProduction())
{
    app.MapOpenApi().AllowAnonymous();
    app.MapScalarApiReference().AllowAnonymous();
}

app.MapHealthChecks("/health/live", new HealthCheckOptions { Predicate = _ => false }).AllowAnonymous().DisableRateLimiting();
app.MapHealthChecks("/health/ready", new HealthCheckOptions { Predicate = c => c.Tags.Contains("ready") }).AllowAnonymous().DisableRateLimiting();

app.MapGroup("/api/v1")
    .MapAccountEndpoints()
    .MapPaymentEndpoints()
    .MapAdminEndpoints();

await app.RunAsync();

/// <summary>Entry point marker for <c>WebApplicationFactory&lt;Program&gt;</c>.</summary>
public partial class Program;
