namespace LedgerFlow.Domain.Common;

/// <summary>
/// Lets generic pipeline code (e.g. validation) build a failed result without knowing the concrete type.
/// </summary>
public interface IResultFactory<TSelf> where TSelf : IResultFactory<TSelf>
{
    static abstract TSelf Failure(Error error);
}

/// <summary>
/// Explicit success/failure for expected outcomes. Exceptions are reserved for truly exceptional states.
/// </summary>
public class Result : IResultFactory<Result>
{
    protected Result(bool isSuccess, Error error)
    {
        if (isSuccess && error != Error.None || !isSuccess && error == Error.None)
        {
            throw new ArgumentException("Invalid result state.", nameof(error));
        }

        IsSuccess = isSuccess;
        Error = error;
    }

    public bool IsSuccess { get; }

    public bool IsFailure => !IsSuccess;

    public Error Error { get; }

    public static Result Success() => new(true, Error.None);

    public static Result Failure(Error error) => new(false, error);

    public static Result<TValue> Success<TValue>(TValue value) => new(value, true, Error.None);

    public static Result<TValue> Failure<TValue>(Error error) => new(default, false, error);

    public static implicit operator Result(Error error) => Failure(error);
}

public sealed class Result<TValue> : Result, IResultFactory<Result<TValue>>
{
    private readonly TValue? _value;

    internal Result(TValue? value, bool isSuccess, Error error)
        : base(isSuccess, error) => _value = value;

    public TValue Value => IsSuccess
        ? _value!
        : throw new InvalidOperationException("The value of a failed result cannot be accessed.");

    public static implicit operator Result<TValue>(TValue value) => new(value, true, Error.None);

    public static implicit operator Result<TValue>(Error error) => new(default, false, error);

    static Result<TValue> IResultFactory<Result<TValue>>.Failure(Error error) => new(default, false, error);
}
