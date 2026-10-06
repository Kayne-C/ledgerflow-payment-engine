using System.Diagnostics;
using LedgerFlow.Api.Security;
using LedgerFlow.Domain.Common;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Mvc;

namespace LedgerFlow.Api.Infrastructure;

/// <summary>Maps application errors to RFC 9457 problem details. One table for every endpoint.</summary>
internal static class HttpResults
{
    public const string IdempotencyKeyHeader = "Idempotency-Key";
    public const string ReplayedHeader = "Idempotent-Replayed";

    public static IResult ToHttp<T>(this Result<T> result, Func<T, IResult> onSuccess) =>
        result.IsSuccess ? onSuccess(result.Value) : result.Error.ToProblem();

    public static IResult ToProblem(this Error error)
    {
        if (error is ValidationError validation)
        {
            return TypedResults.ValidationProblem(
                validation.Errors.ToDictionary(kv => kv.Key, kv => kv.Value),
                title: error.Description,
                extensions: new Dictionary<string, object?> { ["code"] = error.Code });
        }

        var status = error.Type switch
        {
            ErrorType.Validation => StatusCodes.Status400BadRequest,
            ErrorType.Unauthorized => StatusCodes.Status401Unauthorized,
            ErrorType.Forbidden => StatusCodes.Status403Forbidden,
            ErrorType.NotFound => StatusCodes.Status404NotFound,
            ErrorType.Conflict => StatusCodes.Status409Conflict,
            ErrorType.PreconditionFailed => StatusCodes.Status412PreconditionFailed,
            _ => StatusCodes.Status422UnprocessableEntity,
        };

        return TypedResults.Problem(
            statusCode: status,
            title: error.Code,
            detail: error.Description,
            extensions: new Dictionary<string, object?> { ["code"] = error.Code });
    }

    public static string ClientId(this HttpContext context) =>
        context.User.FindFirst(ApiScopes.ClientIdClaim)?.Value ?? throw new InvalidOperationException("Unauthenticated request reached a protected endpoint.");

    /// <summary>Money movements must be retry-safe; the key scopes the request to the calling client.</summary>
    public static bool TryGetIdempotencyKey(this HttpRequest request, out string key, out IResult? problem)
    {
        key = request.Headers[IdempotencyKeyHeader].ToString().Trim();
        if (key.Length is > 0 and <= 100)
        {
            problem = null;
            return true;
        }

        problem = TypedResults.ValidationProblem(
            new Dictionary<string, string[]> { [IdempotencyKeyHeader] = ["A unique Idempotency-Key header (1–100 characters) is required."] },
            title: "Idempotency key required.");
        return false;
    }

    public static void MarkReplayed(this HttpContext context, bool replayed)
    {
        if (replayed)
        {
            context.Response.Headers[ReplayedHeader] = "true";
        }
    }
}

internal sealed partial class GlobalExceptionHandler(IProblemDetailsService problemDetails, ILogger<GlobalExceptionHandler> logger) : IExceptionHandler
{
    public async ValueTask<bool> TryHandleAsync(HttpContext httpContext, Exception exception, CancellationToken cancellationToken)
    {
        var (status, title) = exception switch
        {
            BadHttpRequestException bad => (bad.StatusCode, "Request.Invalid"),
            Application.Abstractions.ConcurrencyConflictException => (StatusCodes.Status409Conflict, "Ledger.Contention"),
            _ => (StatusCodes.Status500InternalServerError, "Server.Error"),
        };

        if (status >= StatusCodes.Status500InternalServerError)
        {
            LogUnhandled(logger, exception, httpContext.Request.Method, httpContext.Request.Path);
        }

        httpContext.Response.StatusCode = status;
        return await problemDetails.TryWriteAsync(new ProblemDetailsContext
        {
            HttpContext = httpContext,
            Exception = exception,
            ProblemDetails = new ProblemDetails
            {
                Status = status,
                Title = title,
                Detail = status >= StatusCodes.Status500InternalServerError ? "An unexpected error occurred." : exception.Message,
                Extensions = { ["traceId"] = Activity.Current?.TraceId.ToString() ?? httpContext.TraceIdentifier },
            },
        });
    }

    [LoggerMessage(Level = LogLevel.Error, Message = "Unhandled exception for {Method} {Path}")]
    private static partial void LogUnhandled(ILogger logger, Exception exception, string method, string path);
}
