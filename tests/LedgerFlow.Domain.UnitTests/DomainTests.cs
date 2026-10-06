using LedgerFlow.Domain.Accounts;
using LedgerFlow.Domain.Common;
using LedgerFlow.Domain.Payments;
using LedgerFlow.Domain.Transactions;
using LedgerFlow.Domain.Values;

namespace LedgerFlow.Domain.UnitTests;

internal static class Make
{
    public static readonly DateTime Now = new(2026, 10, 6, 12, 0, 0, DateTimeKind.Utc);
    public static readonly Currency Try = Currency.From("TRY").Value;

    public static Money Lira(decimal amount) => Money.FromMajor(amount, Try).Value;

    public static Account Customer(decimal balance = 0)
    {
        var account = Account.Open(Guid.NewGuid(), "Alice", Try, AccountKind.Customer, Now).Value;
        if (balance > 0)
        {
            account.Credit(Guid.NewGuid(), Lira(balance));
        }

        return account;
    }
}

public sealed class MoneyTests
{
    [Theory]
    [InlineData(10.10, "TRY", 1010)]
    [InlineData(0.01, "USD", 1)]
    [InlineData(1500, "JPY", 1500)]
    public void Amounts_are_stored_as_integer_minor_units(decimal amount, string currency, long minorUnits) =>
        Assert.Equal(minorUnits, Money.FromMajor(amount, currency).Value.MinorUnits);

    [Theory]
    [InlineData(10.001, "TRY")]
    [InlineData(1.5, "JPY")]
    public void More_decimals_than_the_currency_allows_are_rejected_not_rounded(decimal amount, string currency) =>
        Assert.Equal("Money.TooManyDecimals", Money.FromMajor(amount, currency).Error.Code);

    [Fact]
    public void Unknown_currencies_are_rejected() =>
        Assert.Equal("Money.UnsupportedCurrency", Currency.From("XYZ").Error.Code);

    [Fact]
    public void Round_trips_to_major_units_without_floating_point_error() =>
        Assert.Equal(0.3m, Money.ToMajor(Make.Lira(0.1m).MinorUnits + Make.Lira(0.2m).MinorUnits, "TRY"));
}

public sealed class AccountTests
{
    [Fact]
    public void Holds_reduce_available_but_not_the_balance()
    {
        var account = Make.Customer(100m);

        Assert.True(account.PlaceHold(Guid.NewGuid(), Make.Lira(30m)).IsSuccess);

        Assert.Equal(10_000, account.BalanceMinor);
        Assert.Equal(3_000, account.HeldMinor);
        Assert.Equal(7_000, account.AvailableMinor);
    }

    [Fact]
    public void Customer_accounts_cannot_hold_or_debit_more_than_available()
    {
        var account = Make.Customer(100m);
        account.PlaceHold(Guid.NewGuid(), Make.Lira(80m));

        Assert.Equal("Account.InsufficientFunds", account.PlaceHold(Guid.NewGuid(), Make.Lira(20.01m)).Error.Code);
        Assert.Equal("Account.InsufficientFunds", account.Debit(Guid.NewGuid(), Make.Lira(20.01m)).Error.Code);
    }

    [Fact]
    public void Clearing_accounts_may_go_negative()
    {
        var clearing = Account.Open(Guid.NewGuid(), "Clearing TRY", Make.Try, AccountKind.Clearing, Make.Now).Value;

        Assert.True(clearing.Debit(Guid.NewGuid(), Make.Lira(1_000m)).IsSuccess);
        Assert.Equal(-100_000, clearing.BalanceMinor);
    }

    [Fact]
    public void Placing_the_same_hold_twice_is_idempotent()
    {
        var account = Make.Customer(100m);
        var holdId = Guid.NewGuid();

        account.PlaceHold(holdId, Make.Lira(60m));
        var eventsAfterFirst = account.UncommittedEvents.Count;
        account.PlaceHold(holdId, Make.Lira(60m));

        Assert.Equal(eventsAfterFirst, account.UncommittedEvents.Count);
        Assert.Equal(6_000, account.HeldMinor);
    }

    [Fact]
    public void Capturing_a_hold_debits_exactly_the_held_amount()
    {
        var account = Make.Customer(100m);
        var holdId = Guid.NewGuid();
        account.PlaceHold(holdId, Make.Lira(42.50m));

        Assert.True(account.CaptureHold(holdId, Guid.NewGuid()).IsSuccess);

        Assert.Equal(5_750, account.BalanceMinor);
        Assert.Equal(0, account.HeldMinor);
        Assert.Equal("Account.HoldNotFound", account.CaptureHold(holdId, Guid.NewGuid()).Error.Code);
    }

    [Fact]
    public void Releasing_an_unknown_hold_is_a_no_op()
    {
        var account = Make.Customer(100m);
        var before = account.UncommittedEvents.Count;

        account.ReleaseHold(Guid.NewGuid());

        Assert.Equal(before, account.UncommittedEvents.Count);
    }

    [Fact]
    public void Frozen_accounts_reject_money_movements()
    {
        var account = Make.Customer(100m);
        account.Freeze("Court order");

        Assert.Equal("Account.Frozen", account.Credit(Guid.NewGuid(), Make.Lira(1m)).Error.Code);
        Assert.Equal("Account.Frozen", account.PlaceHold(Guid.NewGuid(), Make.Lira(1m)).Error.Code);
    }

    [Fact]
    public void Currency_mismatch_is_rejected()
    {
        var account = Make.Customer(100m);

        Assert.Equal("Money.CurrencyMismatch", account.Credit(Guid.NewGuid(), Money.FromMajor(1m, "EUR").Value).Error.Code);
    }

    [Fact]
    public void Snapshot_restores_the_full_state()
    {
        var account = Make.Customer(100m);
        var holdId = Guid.NewGuid();
        account.PlaceHold(holdId, Make.Lira(25m));

        var restored = Account.FromSnapshot(account.ToSnapshot(), version: 2, lastHash: "abc");

        Assert.Equal((account.BalanceMinor, account.HeldMinor, account.Holder), (restored.BalanceMinor, restored.HeldMinor, restored.Holder));
        Assert.Equal(2, restored.Version);
        Assert.True(restored.CaptureHold(holdId, Guid.NewGuid()).IsSuccess);
    }

    [Fact]
    public void Replaying_events_rebuilds_the_same_state()
    {
        var original = Make.Customer(100m);
        original.PlaceHold(Guid.NewGuid(), Make.Lira(10m));

        var replayed = Account.CreateEmpty();
        var version = 0L;
        foreach (var domainEvent in original.UncommittedEvents)
        {
            replayed.Replay(domainEvent, version++, "hash");
        }

        Assert.Equal((original.Id, original.BalanceMinor, original.HeldMinor), (replayed.Id, replayed.BalanceMinor, replayed.HeldMinor));
        Assert.Equal(2, replayed.Version);
    }
}

public sealed class PaymentTests
{
    private static Payment NewPayment(Guid? from = null, Guid? to = null) => Payment.Initiate(
        Guid.NewGuid(), "client", "key-1", "fp", from ?? Guid.NewGuid(), to ?? Guid.NewGuid(), Make.Lira(10m), "order-1", Make.Now).Value;

    [Fact]
    public void Happy_path_moves_through_every_state()
    {
        var payment = NewPayment();

        Assert.True(payment.MarkFundsReserved().IsSuccess);
        Assert.True(payment.RecordRiskDecision(true, 5, null).IsSuccess);
        Assert.True(payment.Complete(payment.Id, Make.Now).IsSuccess);

        Assert.Equal(PaymentStatus.Completed, payment.Status);
        Assert.True(payment.IsTerminal);
    }

    [Fact]
    public void Steps_cannot_be_skipped_or_repeated()
    {
        var payment = NewPayment();

        Assert.Equal("Payment.InvalidTransition", payment.Complete(Guid.NewGuid(), Make.Now).Error.Code);
        payment.MarkFundsReserved();
        Assert.Equal("Payment.InvalidTransition", payment.MarkFundsReserved().Error.Code);
    }

    [Fact]
    public void Risk_rejection_keeps_funds_held_until_compensation()
    {
        var payment = NewPayment();
        payment.MarkFundsReserved();

        payment.RecordRiskDecision(false, 95, "velocity");

        Assert.Equal(PaymentStatus.RiskRejected, payment.Status);
        Assert.True(payment.HoldsFunds);
        Assert.True(payment.Fail("Risk.Rejected", "velocity", Make.Now).IsSuccess);
        Assert.False(payment.HoldsFunds);
    }

    [Fact]
    public void Terminal_payments_cannot_fail_again()
    {
        var payment = NewPayment();
        payment.Fail("X", "x", Make.Now);

        Assert.True(payment.Fail("Y", "y", Make.Now).IsFailure);
    }

    [Fact]
    public void Self_transfers_are_rejected()
    {
        var account = Guid.NewGuid();

        var result = Payment.Initiate(Guid.NewGuid(), "c", "k", "fp", account, account, Make.Lira(1m), null, Make.Now);

        Assert.Equal("Payment.SameAccount", result.Error.Code);
    }
}

public sealed class LedgerTransactionTests
{
    [Fact]
    public void Balanced_postings_are_accepted()
    {
        var result = LedgerTransaction.Post(
            Guid.NewGuid(),
            TransactionKind.Payment,
            Make.Try,
            [new Posting(Guid.NewGuid(), PostingDirection.Debit, 500), new Posting(Guid.NewGuid(), PostingDirection.Credit, 500)],
            "fp",
            null,
            Make.Now);

        Assert.True(result.IsSuccess);
    }

    [Fact]
    public void Unbalanced_postings_violate_double_entry()
    {
        var result = LedgerTransaction.Post(
            Guid.NewGuid(),
            TransactionKind.Payment,
            Make.Try,
            [new Posting(Guid.NewGuid(), PostingDirection.Debit, 500), new Posting(Guid.NewGuid(), PostingDirection.Credit, 499)],
            "fp",
            null,
            Make.Now);

        Assert.Equal("Transaction.Unbalanced", result.Error.Code);
    }

    [Fact]
    public void A_single_leg_is_not_a_transaction() =>
        Assert.Equal(
            "Transaction.TooFewPostings",
            LedgerTransaction.Post(Guid.NewGuid(), TransactionKind.Deposit, Make.Try, [new Posting(Guid.NewGuid(), PostingDirection.Credit, 1)], "fp", null, Make.Now).Error.Code);
}

public sealed class IntegrityPrimitivesTests
{
    [Fact]
    public void Changing_any_input_changes_the_hash()
    {
        var baseline = HashChain.Compute(null, "account-1", 0, "AccountOpened", "{\"a\":1}");

        Assert.Equal(64, baseline.Length);
        Assert.NotEqual(baseline, HashChain.Compute(null, "account-1", 0, "AccountOpened", "{\"a\":2}"));
        Assert.NotEqual(baseline, HashChain.Compute(null, "account-1", 1, "AccountOpened", "{\"a\":1}"));
        Assert.NotEqual(baseline, HashChain.Compute("x", "account-1", 0, "AccountOpened", "{\"a\":1}"));
    }

    [Fact]
    public void Deterministic_ids_are_stable_per_client_and_key()
    {
        var first = Deterministic.Id("payment", "client-a", "key-1");

        Assert.Equal(first, Deterministic.Id("payment", "client-a", "key-1"));
        Assert.NotEqual(first, Deterministic.Id("payment", "client-b", "key-1"));
        Assert.NotEqual(first, Deterministic.Id("deposit", "client-a", "key-1"));
    }
}
