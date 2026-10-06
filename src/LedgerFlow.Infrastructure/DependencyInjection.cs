using LedgerFlow.Application.Abstractions;
using LedgerFlow.Application.Abstractions.Messaging;
using LedgerFlow.Application.Features.Accounts;
using LedgerFlow.Application.Features.Ledger;
using LedgerFlow.Contracts.Risk;
using LedgerFlow.Domain.Transactions;
using LedgerFlow.Infrastructure.EventStore;
using LedgerFlow.Infrastructure.Messaging;
using LedgerFlow.Infrastructure.Persistence;
using LedgerFlow.Infrastructure.Risk;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LedgerFlow.Infrastructure;

public static class DependencyInjection
{
    public const string SqlServerMigrationsAssembly = "LedgerFlow.Migrations.SqlServer";
    public const string OracleMigrationsAssembly = "LedgerFlow.Migrations.Oracle";

    public static IServiceCollection AddInfrastructure(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddOptions<DatabaseOptions>().Bind(configuration.GetSection(DatabaseOptions.Section));
        services.AddOptions<MessagingOptions>().Bind(configuration.GetSection(MessagingOptions.Section));
        services.AddOptions<EventStoreOptions>().Bind(configuration.GetSection(EventStoreOptions.Section));
        services.AddOptions<RiskOptions>().Bind(configuration.GetSection(RiskOptions.Section));
        services.TryAddSingleton(TimeProvider.System);

        // Pooled: the context has no scoped dependencies, so instances are reset and reused instead of rebuilt
        // for every request and every consumed message.
        services.AddDbContextPool<LedgerDbContext>((sp, options) =>
        {
            var database = sp.GetRequiredService<IOptions<DatabaseOptions>>().Value;
            switch (database.Provider)
            {
                case DatabaseProvider.SqlServer:
                    options.UseSqlServer(database.ConnectionString, sql => sql
                        .MigrationsAssembly(SqlServerMigrationsAssembly)
                        .EnableRetryOnFailure(maxRetryCount: 5));
                    break;
                case DatabaseProvider.Oracle:
                    options.UseOracle(database.ConnectionString, oracle => oracle.MigrationsAssembly(OracleMigrationsAssembly));
                    break;
                default:
                    options.UseSqlite(database.ConnectionString);
                    break;
            }
        });

        services.AddSingleton<AccountStateCache>();
        services.AddScoped<ILedgerSession, LedgerSession>();
        services.AddScoped<ILedgerReadStore, ReadStore>();
        services.AddScoped<IEventStoreAudit, EventStoreAudit>();

        AddMessaging(services);
        AddRisk(services, configuration);
        return services;
    }

    /// <summary>Outbox relay, Kafka topology + consumers and the stalled-payment sweeper; each self-disables via options.</summary>
    public static IServiceCollection AddLedgerBackgroundServices(this IServiceCollection services)
    {
        services.AddHostedService<KafkaTopologyInitializer>();
        services.AddHostedService(sp => sp.GetRequiredService<OutboxRelay>());
        services.AddHostedService<KafkaConsumerHost>();
        services.AddHostedService<StalledPaymentSweeper>();
        return services;
    }

    public static IHealthChecksBuilder AddInfrastructureHealthChecks(this IHealthChecksBuilder builder) => builder
        .AddDbContextCheck<LedgerDbContext>("database", tags: ["ready"])
        .AddCheck<KafkaHealthCheck>("kafka", tags: ["ready"]);

    public static async Task InitializeDatabaseAsync(this IServiceProvider services, CancellationToken cancellationToken = default)
    {
        await using var scope = services.CreateAsyncScope();
        var options = scope.ServiceProvider.GetRequiredService<IOptions<DatabaseOptions>>().Value;
        var db = scope.ServiceProvider.GetRequiredService<LedgerDbContext>();

        if (options.ApplyMigrationsOnStartup)
        {
            if (options.Provider == DatabaseProvider.Sqlite)
            {
                await db.Database.EnsureCreatedAsync(cancellationToken);
            }
            else
            {
                await db.Database.MigrateAsync(cancellationToken);
            }
        }

        if (options.SeedDemoData && !await db.AccountViews.AnyAsync(cancellationToken))
        {
            await SeedDemoAccountsAsync(scope.ServiceProvider.GetRequiredService<ISender>(), cancellationToken);
        }
    }

    private static async Task SeedDemoAccountsAsync(ISender sender, CancellationToken cancellationToken)
    {
        foreach (var (holder, deposit) in new[] { ("Alice Yılmaz", 25_000m), ("Bora Kaya", 5_000m), ("Kapadokya Market Ltd.", 0m) })
        {
            var account = (await sender.Send(new OpenAccountCommand(holder, "TRY"), cancellationToken)).Value;
            if (deposit > 0)
            {
                await sender.Send(
                    new MoveFundsCommand(TransactionKind.Deposit, "seed", $"seed-{account.AccountId:N}", account.AccountId, deposit, "TRY", "Opening balance"),
                    cancellationToken);
            }
        }
    }

    private static void AddMessaging(IServiceCollection services)
    {
        services.AddSingleton<MessageDispatcher>();
        services.AddSingleton<InMemoryMessageBus>();
        services.AddSingleton<KafkaMessageBus>();
        services.AddSingleton<IMessageBus>(sp =>
            sp.GetRequiredService<IOptions<MessagingOptions>>().Value.Transport == MessageTransport.Kafka
                ? sp.GetRequiredService<KafkaMessageBus>()
                : sp.GetRequiredService<InMemoryMessageBus>());

        services.AddSingleton<OutboxRelay>();
        services.AddSingleton<IOutboxRelay>(sp => sp.GetRequiredService<OutboxRelay>());
    }

    private static void AddRisk(IServiceCollection services, IConfiguration configuration)
    {
        var risk = configuration.GetSection(RiskOptions.Section).Get<RiskOptions>() ?? new RiskOptions();
        if (risk.Mode != RiskMode.Grpc)
        {
            services.AddSingleton<IRiskAssessor, ApproveAllRiskAssessor>();
            return;
        }

        services.AddGrpcClient<RiskEngine.RiskEngineClient>(o => o.Address = risk.Address)
            .AddStandardResilienceHandler(o =>
            {
                o.AttemptTimeout.Timeout = risk.AttemptTimeout;
                o.TotalRequestTimeout.Timeout = risk.TotalTimeout;
                o.CircuitBreaker.SamplingDuration = TimeSpan.FromSeconds(Math.Max(10, risk.AttemptTimeout.TotalSeconds * 2));
                o.Retry.MaxRetryAttempts = 3;
            });
        services.AddScoped<IRiskAssessor, GrpcRiskAssessor>();
    }
}

/// <summary>Periodically re-drives payments whose next step was lost (DLQ, outage) and times out abandoned ones.</summary>
public sealed partial class StalledPaymentSweeper(
    IServiceScopeFactory scopeFactory,
    IOptions<MessagingOptions> options,
    TimeProvider clock,
    ILogger<StalledPaymentSweeper> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var sweeper = options.Value.Sweeper;
        if (!sweeper.Enabled)
        {
            return;
        }

        using var timer = new PeriodicTimer(sweeper.Interval, clock);
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                await using var scope = scopeFactory.CreateAsyncScope();
                var result = await scope.ServiceProvider.GetRequiredService<ISender>()
                    .Send(new RedriveStalledPaymentsCommand(sweeper.StalledFor, sweeper.HardTimeout), stoppingToken);

                if (result.IsSuccess && result.Value.Redriven + result.Value.TimedOut > 0)
                {
                    LogSwept(logger, result.Value.Redriven, result.Value.TimedOut);
                }
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                LogSweepFailed(logger, exception);
            }
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Re-drove {Redriven} stalled payment(s), timed out {TimedOut}")]
    private static partial void LogSwept(ILogger logger, int redriven, int timedOut);

    [LoggerMessage(Level = LogLevel.Error, Message = "Stalled payment sweep failed")]
    private static partial void LogSweepFailed(ILogger logger, Exception exception);
}
