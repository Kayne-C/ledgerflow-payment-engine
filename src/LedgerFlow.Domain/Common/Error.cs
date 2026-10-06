namespace LedgerFlow.Domain.Common;

public enum ErrorType
{
    BusinessRule,
    Validation,
    NotFound,
    Conflict,
    Unauthorized,
    Forbidden,
    PreconditionFailed,
}

public record Error(string Code, string Description, ErrorType Type = ErrorType.BusinessRule)
{
    public static readonly Error None = new(string.Empty, string.Empty);

    public static Error BusinessRule(string code, string description) => new(code, description);

    public static Error NotFound(string code, string description) => new(code, description, ErrorType.NotFound);

    public static Error Conflict(string code, string description) => new(code, description, ErrorType.Conflict);

    public static Error Unauthorized(string code, string description) => new(code, description, ErrorType.Unauthorized);

    public static Error Forbidden(string code, string description) => new(code, description, ErrorType.Forbidden);

    public static Error PreconditionFailed(string code, string description) =>
        new(code, description, ErrorType.PreconditionFailed);
}

public sealed record ValidationError(IReadOnlyDictionary<string, string[]> Errors)
    : Error("Validation.Failed", "One or more validation errors occurred.", ErrorType.Validation);
