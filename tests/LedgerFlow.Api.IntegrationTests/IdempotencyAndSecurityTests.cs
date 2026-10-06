using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using LedgerFlow.Application.Abstractions;
using LedgerFlow.Application.Features.Accounts;

namespace LedgerFlow.Api.IntegrationTests;

[Collection(ApiCollection.Name)]
public sealed class IdempotencyAndSecurityTests(LedgerApiFactory factory)
{
    [Fact]
    public async Task Retrying_a_payment_with_the_same_key_returns_the_original_payment()
    {
        using var client = factory.Client();
        var from = await factory.OpenAccountAsync(client, 100m);
        var to = await factory.OpenAccountAsync(client);

        var (_, first) = await LedgerApiFactory.PayAsync(client, from, to, 25m, key: "order-42", reference: "order 42");
        var (retryResponse, retry) = await LedgerApiFactory.PayAsync(client, from, to, 25m, key: "order-42", reference: "order 42");
        await factory.DrainAsync();

        Assert.Equal(first!.PaymentId, retry!.PaymentId);
        Assert.Equal("true", retryResponse.Headers.GetValues("Idempotent-Replayed").Single());
        Assert.Equal(75m, (await LedgerApiFactory.GetAsync<AccountResponse>(client, $"/api/v1/accounts/{from}")).Balance);
    }

    [Fact]
    public async Task Reusing_a_key_for_a_different_request_is_rejected()
    {
        using var client = factory.Client();
        var from = await factory.OpenAccountAsync(client, 100m);
        var to = await factory.OpenAccountAsync(client);
        await LedgerApiFactory.PayAsync(client, from, to, 25m, key: "order-43");

        var (response, _) = await LedgerApiFactory.PayAsync(client, from, to, 26m, key: "order-43");

        Assert.Equal(HttpStatusCode.UnprocessableEntity, response.StatusCode);
        Assert.Contains("Idempotency.KeyReused", await response.Content.ReadAsStringAsync(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Idempotency_keys_are_scoped_per_client()
    {
        using var ops = factory.Client();
        using var merchant = factory.Client(LedgerApiFactory.MerchantKey);
        var from = await factory.OpenAccountAsync(ops, 100m);
        var to = await factory.OpenAccountAsync(ops);

        var (_, a) = await LedgerApiFactory.PayAsync(ops, from, to, 10m, key: "shared-key");
        var (_, b) = await LedgerApiFactory.PayAsync(merchant, from, to, 10m, key: "shared-key");

        Assert.NotEqual(a!.PaymentId, b!.PaymentId);
    }

    [Fact]
    public async Task Deposits_are_idempotent_and_require_a_key()
    {
        using var client = factory.Client();
        var account = await factory.OpenAccountAsync(client);
        var url = $"/api/v1/accounts/{account}/deposits";

        using var first = LedgerApiFactory.Post(url, new { amount = 99.99m, currency = "TRY" }, "dep-1");
        using var again = LedgerApiFactory.Post(url, new { amount = 99.99m, currency = "TRY" }, "dep-1");
        using var missingKey = LedgerApiFactory.Post(url, new { amount = 1m, currency = "TRY" }, null);

        Assert.Equal(HttpStatusCode.Created, (await client.SendAsync(first)).StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(again)).StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, (await client.SendAsync(missingKey)).StatusCode);
        Assert.Equal(99.99m, (await LedgerApiFactory.GetAsync<AccountResponse>(client, $"/api/v1/accounts/{account}")).Balance);
    }

    [Fact]
    public async Task Withdrawals_never_overdraw()
    {
        using var client = factory.Client();
        var account = await factory.OpenAccountAsync(client, 50m);

        using var request = LedgerApiFactory.Post($"/api/v1/accounts/{account}/withdrawals", new { amount = 50.01m, currency = "TRY" }, "wd-1");
        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.UnprocessableEntity, response.StatusCode);
        Assert.Contains("Account.InsufficientFunds", await response.Content.ReadAsStringAsync(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(10.001)]
    [InlineData(-5)]
    public async Task Invalid_amounts_are_rejected_before_any_state_change(double amount)
    {
        using var client = factory.Client();
        var from = await factory.OpenAccountAsync(client, 100m);
        var to = await factory.OpenAccountAsync(client);

        var (response, _) = await LedgerApiFactory.PayAsync(client, from, to, (decimal)amount);

        Assert.True(response.StatusCode is HttpStatusCode.BadRequest or HttpStatusCode.UnprocessableEntity);
    }

    [Fact]
    public async Task Requests_need_a_valid_key_and_admin_endpoints_need_the_admin_scope()
    {
        using var anonymous = factory.CreateClient();
        using var wrongKey = factory.Client("not-a-key");
        using var merchant = factory.Client(LedgerApiFactory.MerchantKey);

        Assert.Equal(HttpStatusCode.Unauthorized, (await anonymous.GetAsync("/api/v1/admin/stats")).StatusCode);
        Assert.Equal(HttpStatusCode.Unauthorized, (await wrongKey.GetAsync("/api/v1/admin/stats")).StatusCode);
        Assert.Equal(HttpStatusCode.Forbidden, (await merchant.GetAsync("/api/v1/admin/stats")).StatusCode);
        Assert.Equal(HttpStatusCode.Created, (await merchant.PostAsJsonAsync("/api/v1/accounts", new { holder = "M", currency = "TRY" })).StatusCode);
    }

    [Fact]
    public async Task Balance_at_a_past_instant_and_stream_integrity_are_queryable()
    {
        using var client = factory.Client();
        var account = await factory.OpenAccountAsync(client, 10m);
        factory.Clock.Advance(TimeSpan.FromHours(1));
        var checkpoint = factory.Clock.GetUtcNow().UtcDateTime;
        factory.Clock.Advance(TimeSpan.FromHours(1));
        using var deposit = LedgerApiFactory.Post($"/api/v1/accounts/{account}/deposits", new { amount = 5m, currency = "TRY" }, Guid.NewGuid().ToString());
        (await client.SendAsync(deposit)).EnsureSuccessStatusCode();

        var historic = await LedgerApiFactory.GetAsync<BalanceAtResponse>(client, $"/api/v1/accounts/{account}/balance-at?at={checkpoint:O}");
        var integrity = await LedgerApiFactory.GetAsync<IntegrityReport>(client, $"/api/v1/admin/streams/account-{account:N}/integrity");

        Assert.Equal(10m, historic.Balance);
        Assert.Equal(15m, (await LedgerApiFactory.GetAsync<AccountResponse>(client, $"/api/v1/accounts/{account}")).Balance);
        Assert.True(integrity.IsIntact);
        Assert.Equal(3, integrity.EventCount);
    }

    [Fact]
    public async Task Problem_details_carry_a_machine_readable_code()
    {
        using var client = factory.Client();

        var response = await client.GetAsync($"/api/v1/accounts/{Guid.NewGuid()}");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        var problem = JsonDocument.Parse(await response.Content.ReadAsStringAsync()).RootElement;
        Assert.Equal("Account.NotFound", problem.GetProperty("code").GetString());
    }
}
