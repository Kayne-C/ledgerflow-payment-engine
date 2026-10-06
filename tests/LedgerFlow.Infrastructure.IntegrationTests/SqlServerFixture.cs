using LedgerFlow.Application;
using LedgerFlow.Application.Abstractions;
using LedgerFlow.Application.Abstractions.Messaging;
using LedgerFlow.Application.Features.Accounts;
using LedgerFlow.Domain.Transactions;
using LedgerFlow.Infrastructure;
using LedgerFlow.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using Testcontainers.MsSql;

namespace LedgerFlow.Infrastructure.IntegrationTests;

/// <summary>
/// Real SQL Server 2022 in a container (Testcontainers). Concurrency semantics — unique-index races, deadlock
/// handling, row locks — are exactly what SQLite cannot reproduce, so these tests only run against the real thing.
/// Without Docker the tests are skipped, not failed.
/// </summary>
public sealed class SqlServerFixture : IAsyncLifetime
{
    private MsSqlContainer? _container;

    public bool Available { get; private set; }

    public string SkipReason { get; private set; } = "Docker is not available.";

    public FakeTimeProvider Clock { get; } = new(new DateTimeOffset(2026, 1, 1, 9, 0, 0, TimeSpan.Zero));

    public ServiceProvider Services { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        try
        {
            _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest").Build();
            await _container.StartAsync();
        }
#pragma warning disable CA1031 // Any Docker failure means "skip", never "fail".
        catch (Exception exception)
#pragma warning restore CA1031
        {
            SkipReason = $"SQL Server container could not start: {exception.Message}";
            return;
        }

        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Database:Provider"] = "SqlServer",
            ["Database:ConnectionString"] = _container.GetConnectionString() + ";Database=LedgerFlowTests",
            ["EventStore:SnapshotEvery"] = "10",
            ["EventStore:AccountCacheSize"] = "0", // exercise the database paths (snapshots, replay), not the cache
            ["Messaging:Transport"] = "InMemory",
        }).Build();

        var services = new ServiceCollection()
            .AddLogging(logging => logging.SetMinimumLevel(LogLevel.Warning))
            .AddSingleton<TimeProvider>(Clock)
            .AddApplication()
            .AddInfrastructure(configuration);

        Services = services.BuildServiceProvider();
        await using (var scope = Services.CreateAsyncScope())
        {
            await scope.ServiceProvider.GetRequiredService<LedgerDbContext>().Database.MigrateAsync();
        }

        Available = true;
    }

    public async ValueTask DisposeAsync()
    {
        if (Services is not null)
        {
            await Services.DisposeAsync();
        }

        if (_container is not null)
        {
            await _container.DisposeAsync();
        }
    }

    public async Task<T> SendAsync<T>(ICommand<T> command)
    {
        await using var scope = Services.CreateAsyncScope();
        var result = await scope.ServiceProvider.GetRequiredService<ISender>().Send(command);
        return result.IsSuccess ? result.Value : throw new InvalidOperationException(result.Error.Code);
    }

    public async Task<TResult> InScopeAsync<TResult>(Func<IServiceProvider, Task<TResult>> work)
    {
        await using var scope = Services.CreateAsyncScope();
        return await work(scope.ServiceProvider);
    }

    public async Task<Guid> OpenFundedAccountAsync(decimal balance)
    {
        var account = await SendAsync(new OpenAccountCommand("Test", "TRY"));
        if (balance > 0)
        {
            await SendAsync(new MoveFundsCommand(TransactionKind.Deposit, "tests", Guid.NewGuid().ToString(), account.AccountId, balance, "TRY", null));
        }

        return account.AccountId;
    }

    public Task<AccountResponse> AccountAsync(Guid accountId) => InScopeAsync(async sp =>
        (await sp.GetRequiredService<ISender>().Send(new GetAccountQuery(accountId))).Value);

    public Task<LedgerDbContext> Db(IServiceProvider scope) => Task.FromResult(scope.GetRequiredService<LedgerDbContext>());

    public ILedgerSession Session(IServiceProvider scope) => scope.GetRequiredService<ILedgerSession>();
}

[CollectionDefinition(Name)]
public sealed class SqlServerCollection : ICollectionFixture<SqlServerFixture>
{
    public const string Name = "sqlserver";
}
