using System.Diagnostics;
using Microsoft.Extensions.Logging;
using LedgerFlow.Application.Abstractions.Messaging;
using LedgerFlow.Application.Diagnostics;
using LedgerFlow.Domain.Common;

namespace LedgerFlow.Application.Abstractions.Behaviors;

/// <summary>One span + one structured log line + one histogram sample per use case.</summary>
internal sealed partial class RequestTelemetryBehavior<TRequest, TResponse>(
    ILogger<RequestTelemetryBehavior<TRequest, TResponse>> logger)
    : IPipelineBehavior<TRequest, TResponse>
    where TRequest : IRequest<TResponse>
{
    public async Task<TResponse> Handle(
        TRequest request,
        RequestHandlerDelegate<TResponse> next,
        CancellationToken cancellationToken)
    {
        var requestName = typeof(TRequest).Name;
        using var activity = LedgerTelemetry.ActivitySource.StartActivity(requestName);
        var startedAt = Stopwatch.GetTimestamp();

        var response = await next();

        var elapsed = Stopwatch.GetElapsedTime(startedAt);
        LedgerTelemetry.RequestDuration.Record(
            elapsed.TotalMilliseconds,
            new KeyValuePair<string, object?>("request", requestName));

        if (response is Result { IsFailure: true } failed)
        {
            activity?.SetStatus(ActivityStatusCode.Error, failed.Error.Code);
            LogFailure(logger, requestName, failed.Error.Code, elapsed.TotalMilliseconds);
        }
        else
        {
            LogSuccess(logger, requestName, elapsed.TotalMilliseconds);
        }

        return response;
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "{Request} completed in {ElapsedMs:0.0} ms")]
    private static partial void LogSuccess(ILogger logger, string request, double elapsedMs);

    [LoggerMessage(Level = LogLevel.Warning, Message = "{Request} failed with {ErrorCode} in {ElapsedMs:0.0} ms")]
    private static partial void LogFailure(ILogger logger, string request, string errorCode, double elapsedMs);
}
