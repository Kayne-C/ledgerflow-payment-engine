using System.Globalization;
using System.Threading.RateLimiting;
using LedgerFlow.Api.Security;
using LedgerFlow.Application.Diagnostics;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.OpenApi;
using Microsoft.AspNetCore.RateLimiting;
using Microsoft.Extensions.Options;
using Microsoft.OpenApi;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace LedgerFlow.Api.Infrastructure;

public sealed class RateLimitingSettings
{
    public const string Section = "RateLimiting";

    public int PermitsPerSecondPerClient { get; set; } = 500;

    public int BurstPerClient { get; set; } = 1000;
}

internal static class ApiServiceExtensions
{
    public static IServiceCollection AddApiKeySecurity(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddOptions<ApiClientsOptions>()
            .Bind(configuration.GetSection(ApiClientsOptions.Section))
            .Validate(o => o.Clients.All(c => c.KeyHash.Length == 64), "Every ApiClients:Clients:*:KeyHash must be a hex SHA-256.")
            .ValidateOnStart();

        services.AddAuthentication(ApiKeyAuthenticationHandler.SchemeName)
            .AddScheme<AuthenticationSchemeOptions, ApiKeyAuthenticationHandler>(ApiKeyAuthenticationHandler.SchemeName, null);

        services.AddAuthorizationBuilder()
            .SetFallbackPolicy(new Microsoft.AspNetCore.Authorization.AuthorizationPolicyBuilder().RequireAuthenticatedUser().Build())
            .AddPolicy(ApiScopes.Payments, p => p.RequireClaim(ApiScopes.ClaimType, ApiScopes.Payments))
            .AddPolicy(ApiScopes.Admin, p => p.RequireClaim(ApiScopes.ClaimType, ApiScopes.Admin));

        return services;
    }

    /// <summary>Token bucket per API client: one integrator's burst cannot starve the others.</summary>
    public static IServiceCollection AddClientRateLimiting(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddOptions<RateLimitingSettings>().Bind(configuration.GetSection(RateLimitingSettings.Section));
        services.AddRateLimiter(_ => { });
        services.AddOptions<RateLimiterOptions>().Configure<IOptions<RateLimitingSettings>>((options, settings) =>
        {
            var limits = settings.Value;
            options.GlobalLimiter = PartitionedRateLimiter.Create<HttpContext, string>(context =>
                RateLimitPartition.GetTokenBucketLimiter(
                    context.User.FindFirst(ApiScopes.ClientIdClaim)?.Value ?? $"ip:{context.Connection.RemoteIpAddress}",
                    _ => new TokenBucketRateLimiterOptions
                    {
                        TokenLimit = limits.BurstPerClient,
                        TokensPerPeriod = limits.PermitsPerSecondPerClient,
                        ReplenishmentPeriod = TimeSpan.FromSeconds(1),
                        QueueLimit = 0,
                    }));

            options.OnRejected = async (rejected, _) =>
            {
                if (rejected.Lease.TryGetMetadata(MetadataName.RetryAfter, out var retryAfter))
                {
                    rejected.HttpContext.Response.Headers.RetryAfter = ((int)Math.Ceiling(retryAfter.TotalSeconds)).ToString(CultureInfo.InvariantCulture);
                }

                await TypedResults.Problem(statusCode: StatusCodes.Status429TooManyRequests, title: "RateLimit.Exceeded").ExecuteAsync(rejected.HttpContext);
            };
        });

        return services;
    }

    public static IServiceCollection AddApiDocumentation(this IServiceCollection services) =>
        services.AddOpenApi(options => options.AddDocumentTransformer<ApiKeySecuritySchemeTransformer>());

    public static WebApplicationBuilder AddObservability(this WebApplicationBuilder builder)
    {
        var otel = builder.Services.AddOpenTelemetry()
            .ConfigureResource(resource => resource.AddService(builder.Environment.ApplicationName))
            .WithTracing(tracing => tracing
                .AddSource(LedgerTelemetry.SourceName)
                .AddAspNetCoreInstrumentation(o => o.Filter = context => !context.Request.Path.StartsWithSegments("/health"))
                .AddHttpClientInstrumentation())
            .WithMetrics(metrics => metrics
                .AddMeter(LedgerTelemetry.SourceName)
                .AddAspNetCoreInstrumentation()
                .AddHttpClientInstrumentation()
                .AddRuntimeInstrumentation());

        builder.Logging.AddOpenTelemetry(logging => logging.IncludeFormattedMessage = true);
        if (!string.IsNullOrWhiteSpace(builder.Configuration["OTEL_EXPORTER_OTLP_ENDPOINT"]))
        {
            otel.UseOtlpExporter();
        }

        return builder;
    }
}

internal sealed class ApiKeySecuritySchemeTransformer : IOpenApiDocumentTransformer
{
    public Task TransformAsync(OpenApiDocument document, OpenApiDocumentTransformerContext context, CancellationToken cancellationToken)
    {
        document.Info = new OpenApiInfo
        {
            Title = "LedgerFlow Payment Engine API",
            Version = "v1",
            Description = "Event-sourced double-entry ledger and payment saga. Authenticate with X-Api-Key; send Idempotency-Key on money movements.",
        };

        document.Components ??= new OpenApiComponents();
        document.Components.SecuritySchemes ??= new Dictionary<string, IOpenApiSecurityScheme>();
        document.Components.SecuritySchemes[ApiKeyAuthenticationHandler.SchemeName] = new OpenApiSecurityScheme
        {
            Type = SecuritySchemeType.ApiKey,
            In = ParameterLocation.Header,
            Name = ApiKeyAuthenticationHandler.HeaderName,
            Description = "Server-to-server API key.",
        };

        document.Security ??= [];
        document.Security.Add(new OpenApiSecurityRequirement
        {
            [new OpenApiSecuritySchemeReference(ApiKeyAuthenticationHandler.SchemeName, document)] = [],
        });
        return Task.CompletedTask;
    }
}
