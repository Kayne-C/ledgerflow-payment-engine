using System.Collections.Concurrent;
using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using LedgerFlow.Application.Abstractions;
using LedgerFlow.Application.Features.Accounts;
using LedgerFlow.Application.Features.Payments;
using LedgerFlow.Infrastructure.Messaging;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;

namespace LedgerFlow.Api.IntegrationTests;

/// <summary>Risk engine double: approves by default, can reject specific amounts or simulate an outage.</summary>
public sealed class ScriptedRiskAssessor : IRiskAssessor
{
    public ConcurrentDictionary<long, string> RejectAmountsMinor { get; } = new();

    public volatile bool Unavailable;

    public Task<RiskDecision> AssessAsync(RiskRequest request, CancellationToken cancellationToken)
    {
        if (Unavailable)
        {
            throw new HttpRequestException("Risk engine unavailable (simulated).");
        }

        return Task.FromResult(RejectAmountsMinor.TryGetValue(request.AmountMinor, out var reason)
            ? new RiskDecision(false, 95, reason)
            : new RiskDecision(true, 3, null));
    }
}

/// <summary>Real API pipeline on in-memory SQLite. The outbox is relayed manually (<see cref="DrainAsync"/>) for deterministic sagas.</summary>
public sealed class LedgerApiFactory : WebApplicationFactory<Program>, IAsyncLifetime
{
    public const string MerchantKey = "test-merchant-key";
    public const string OpsKey = "test-ops-key";

    public static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web) { Converters = { new JsonStringEnumConverter() } };

    private readonly string _connectionString = $"Data Source=file:ledgerflow-{Guid.NewGuid():N}?mode=memory&cache=shared";
    private readonly SqliteConnection _keepAlive;

    public LedgerApiFactory()
    {
        _keepAlive = new SqliteConnection(_connectionString);
        _keepAlive.Open();
    }

    public ScriptedRiskAssessor Risk { get; } = new();

    public FakeTimeProvider Clock { get; } = new(new DateTimeOffset(2026, 3, 1, 8, 0, 0, TimeSpan.Zero));

    public ValueTask InitializeAsync()
    {
        _ = Server;
        return ValueTask.CompletedTask;
    }

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        await _keepAlive.DisposeAsync();
    }

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Testing");
        builder.ConfigureAppConfiguration((_, config) => config.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Database:Provider"] = "Sqlite",
            ["Database:ConnectionString"] = _connectionString,
            ["Database:ApplyMigrationsOnStartup"] = "true",
            ["Messaging:Transport"] = "InMemory",
            ["Messaging:Outbox:Enabled"] = "false",
            ["ApiClients:Clients:0:ClientId"] = "merchant",
            ["ApiClients:Clients:0:KeyHash"] = Hash(MerchantKey),
            ["ApiClients:Clients:0:Scopes:0"] = "payments",
            ["ApiClients:Clients:1:ClientId"] = "ops",
            ["ApiClients:Clients:1:KeyHash"] = Hash(OpsKey),
            ["ApiClients:Clients:1:Scopes:0"] = "payments",
            ["ApiClients:Clients:1:Scopes:1"] = "admin",
            ["RateLimiting:PermitsPerSecondPerClient"] = "100000",
            ["RateLimiting:BurstPerClient"] = "100000",
        }));

        builder.ConfigureTestServices(services =>
        {
            services.AddSingleton<IRiskAssessor>(Risk);
            services.AddSingleton<TimeProvider>(Clock);
        });
    }

    public HttpClient Client(string key = OpsKey)
    {
        var client = CreateClient();
        client.DefaultRequestHeaders.Add("X-Api-Key", key);
        return client;
    }

    /// <summary>Relays the outbox until no message is left (each relay step runs the next saga step in-process).</summary>
    public async Task DrainAsync()
    {
        var relay = Services.GetRequiredService<IOutboxRelay>();
        for (var round = 0; round < 50 && await relay.RelayBatchAsync(CancellationToken.None) > 0; round++)
        {
        }
    }

    public async Task<Guid> OpenAccountAsync(HttpClient client, decimal deposit = 0, string currency = "TRY")
    {
        var response = await client.PostAsJsonAsync("/api/v1/accounts", new { holder = "Test Holder", currency });
        response.EnsureSuccessStatusCode();
        var account = (await response.Content.ReadFromJsonAsync<AccountResponse>(Json))!;
        if (deposit > 0)
        {
            using var request = Post($"/api/v1/accounts/{account.AccountId}/deposits", new { amount = deposit, currency }, Guid.NewGuid().ToString());
            (await client.SendAsync(request)).EnsureSuccessStatusCode();
        }

        return account.AccountId;
    }

    public static HttpRequestMessage Post(string url, object body, string? idempotencyKey)
    {
        var request = new HttpRequestMessage(HttpMethod.Post, url) { Content = JsonContent.Create(body) };
        if (idempotencyKey is not null)
        {
            request.Headers.Add("Idempotency-Key", idempotencyKey);
        }

        return request;
    }

    public static async Task<(HttpResponseMessage Response, PaymentResponse? Payment)> PayAsync(
        HttpClient client, Guid from, Guid to, decimal amount, string? key = null, string? reference = null)
    {
        using var request = Post("/api/v1/payments", new { fromAccountId = from, toAccountId = to, amount, currency = "TRY", reference }, key ?? Guid.NewGuid().ToString());
        var response = await client.SendAsync(request);
        var payment = response.IsSuccessStatusCode ? await response.Content.ReadFromJsonAsync<PaymentResponse>(Json) : null;
        return (response, payment);
    }

    public static async Task<T> GetAsync<T>(HttpClient client, string url) => (await client.GetFromJsonAsync<T>(url, Json))!;

    private static string Hash(string key) => Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(key)));
}

[CollectionDefinition(Name)]
public sealed class ApiCollection : ICollectionFixture<LedgerApiFactory>
{
    public const string Name = "api";
}
