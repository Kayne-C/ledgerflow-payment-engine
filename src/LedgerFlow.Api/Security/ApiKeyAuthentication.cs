using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Options;

namespace LedgerFlow.Api.Security;

public sealed class ApiClient
{
    public string ClientId { get; set; } = string.Empty;

    /// <summary>Lower-case hex SHA-256 of the key. Plain keys are never stored or logged.</summary>
    public string KeyHash { get; set; } = string.Empty;

    public string[] Scopes { get; set; } = [];
}

public sealed class ApiClientsOptions
{
    public const string Section = "ApiClients";

    public List<ApiClient> Clients { get; set; } = [];
}

public static class ApiScopes
{
    public const string Payments = "payments";
    public const string Admin = "admin";
    public const string ClaimType = "scope";
    public const string ClientIdClaim = "client_id";
}

/// <summary>
/// Server-to-server API keys (as payment providers issue them). The presented key is hashed and compared in
/// constant time against every configured hash, so neither timing nor storage reveals valid keys.
/// </summary>
internal sealed class ApiKeyAuthenticationHandler(
    IOptionsMonitor<AuthenticationSchemeOptions> options,
    ILoggerFactory logger,
    UrlEncoder encoder,
    IOptionsMonitor<ApiClientsOptions> clients) : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
{
    public const string SchemeName = "ApiKey";
    public const string HeaderName = "X-Api-Key";

    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        if (!Request.Headers.TryGetValue(HeaderName, out var presented) || string.IsNullOrWhiteSpace(presented))
        {
            return Task.FromResult(AuthenticateResult.NoResult());
        }

        Span<byte> presentedHash = stackalloc byte[32];
        SHA256.HashData(Encoding.UTF8.GetBytes(presented.ToString()), presentedHash);

        ApiClient? match = null;
        Span<byte> configuredHash = stackalloc byte[32];
        foreach (var client in clients.CurrentValue.Clients)
        {
            if (Convert.FromHexString(client.KeyHash, configuredHash, out _, out var written) == System.Buffers.OperationStatus.Done
                && written == 32
                && CryptographicOperations.FixedTimeEquals(presentedHash, configuredHash))
            {
                match = client;
            }
        }

        if (match is null)
        {
            return Task.FromResult(AuthenticateResult.Fail("Invalid API key."));
        }

        var claims = new List<Claim> { new(ApiScopes.ClientIdClaim, match.ClientId), new(ClaimTypes.Name, match.ClientId) };
        claims.AddRange(match.Scopes.Select(scope => new Claim(ApiScopes.ClaimType, scope)));
        var principal = new ClaimsPrincipal(new ClaimsIdentity(claims, SchemeName));
        return Task.FromResult(AuthenticateResult.Success(new AuthenticationTicket(principal, SchemeName)));
    }
}
