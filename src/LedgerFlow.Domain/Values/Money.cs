using System.Globalization;
using LedgerFlow.Domain.Common;

namespace LedgerFlow.Domain.Values;

/// <summary>ISO 4217 currency with its minor-unit exponent (TRY/USD/EUR: 2 → kuruş/cent, JPY: 0).</summary>
public readonly record struct Currency
{
    private static readonly Dictionary<string, int> Exponents = new(StringComparer.Ordinal)
    {
        ["TRY"] = 2,
        ["USD"] = 2,
        ["EUR"] = 2,
        ["GBP"] = 2,
        ["JPY"] = 0,
    };

    private Currency(string code) => Code = code;

    public string Code { get; }

    public int Exponent => Exponents[Code];

    public static IReadOnlyCollection<string> Supported => Exponents.Keys;

    public static Result<Currency> From(string? code)
    {
        var normalized = code?.Trim().ToUpperInvariant();
        return normalized is not null && Exponents.ContainsKey(normalized)
            ? new Currency(normalized)
            : MoneyErrors.UnsupportedCurrency(code);
    }

    public override string ToString() => Code;
}

/// <summary>
/// Amount in integer minor units. Money never touches floating point: "10.10 TRY" is stored as 1010 kuruş,
/// and an amount with more decimals than the currency allows is rejected instead of silently rounded.
/// </summary>
public readonly record struct Money(long MinorUnits, Currency Currency)
{
    public static Result<Money> FromMajor(decimal amount, Currency currency)
    {
        var scaled = amount * Pow10(currency.Exponent);
        if (scaled != decimal.Truncate(scaled))
        {
            return MoneyErrors.TooManyDecimals(currency);
        }

        if (scaled is > long.MaxValue or < long.MinValue)
        {
            return MoneyErrors.OutOfRange;
        }

        return new Money((long)scaled, currency);
    }

    public static Result<Money> FromMajor(decimal amount, string currencyCode)
    {
        var currency = Currency.From(currencyCode);
        return currency.IsFailure ? currency.Error : FromMajor(amount, currency.Value);
    }

    public decimal ToMajor() => MinorUnits / Pow10(Currency.Exponent);

    public bool IsPositive => MinorUnits > 0;

    public override string ToString() =>
        string.Create(CultureInfo.InvariantCulture, $"{ToMajor().ToString($"F{Currency.Exponent}", CultureInfo.InvariantCulture)} {Currency.Code}");

    public static decimal ToMajor(long minorUnits, string currencyCode) =>
        minorUnits / Pow10(Currency.From(currencyCode).Value.Exponent);

    private static decimal Pow10(int exponent) => exponent switch
    {
        0 => 1m,
        2 => 100m,
        3 => 1000m,
        _ => (decimal)Math.Pow(10, exponent),
    };
}

public static class MoneyErrors
{
    public static readonly Error OutOfRange = Error.BusinessRule("Money.OutOfRange", "The amount is out of range.");

    public static readonly Error NotPositive = Error.BusinessRule("Money.NotPositive", "The amount must be greater than zero.");

    public static Error UnsupportedCurrency(string? code) =>
        Error.BusinessRule("Money.UnsupportedCurrency", $"Currency '{code}' is not supported. Supported: {string.Join(", ", Currency.Supported)}.");

    public static Error TooManyDecimals(Currency currency) => Error.BusinessRule(
        "Money.TooManyDecimals", $"{currency.Code} amounts may have at most {currency.Exponent} decimal places.");

    public static Error CurrencyMismatch(string expected, string actual) =>
        Error.BusinessRule("Money.CurrencyMismatch", $"Expected {expected} but got {actual}.");
}
