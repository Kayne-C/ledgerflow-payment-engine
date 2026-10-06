using System.Security.Cryptography;
using System.Text;

namespace LedgerFlow.Domain.Common;

public static class Deterministic
{
    /// <summary>
    /// Name-based id (RFC 9562 v8 layout over SHA-256): the same client + idempotency key always maps to the same
    /// payment/transaction stream, which is what makes retries idempotent at the storage level.
    /// </summary>
    public static Guid Id(string scope, string clientId, string key)
    {
        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(Encoding.UTF8.GetBytes($"{scope}\n{clientId}\n{key}"), hash);

        var bytes = hash[..16].ToArray();
        bytes[6] = (byte)((bytes[6] & 0x0F) | 0x80); // version 8 (custom)
        bytes[8] = (byte)((bytes[8] & 0x3F) | 0x80); // RFC variant
        return new Guid(bytes, bigEndian: true);
    }

    /// <summary>Stable fingerprint of a request's business fields (order-sensitive, culture-invariant).</summary>
    public static string Fingerprint(params object?[] parts)
    {
        var text = string.Join('\u001F', parts.Select(p => Convert.ToString(p, System.Globalization.CultureInfo.InvariantCulture)));
        return Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(text)));
    }
}

/// <summary>
/// Per-stream hash chain: hash(n) = SHA-256(hash(n-1) | stream | version | type | payload).
/// Changing, deleting or reordering any stored event breaks every hash after it.
/// </summary>
public static class HashChain
{
    public static string Compute(string? previousHash, string streamId, long version, string eventType, string payload)
    {
        var material = $"{previousHash}\n{streamId}\n{version}\n{eventType}\n{payload}";
        return Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(material)));
    }
}
