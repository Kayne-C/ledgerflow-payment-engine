using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;

namespace LedgerFlow.Application.Abstractions.Messaging;

/// <summary>
/// Minimal in-process mediator. Reflection is paid once per request type; afterwards dispatch is a
/// dictionary lookup plus a virtual call. Keeps the core free of third-party mediator licensing.
/// </summary>
internal sealed class Sender(IServiceProvider serviceProvider) : ISender
{
    private static readonly ConcurrentDictionary<Type, object> Pipelines = new();

    public Task<TResponse> Send<TResponse>(IRequest<TResponse> request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var pipeline = (RequestPipeline<TResponse>)Pipelines.GetOrAdd(
            request.GetType(),
            static requestType => Activator.CreateInstance(
                typeof(RequestPipeline<,>).MakeGenericType(requestType, typeof(TResponse)))!);

        return pipeline.Handle(request, serviceProvider, cancellationToken);
    }
}

internal abstract class RequestPipeline<TResponse>
{
    public abstract Task<TResponse> Handle(
        IRequest<TResponse> request,
        IServiceProvider serviceProvider,
        CancellationToken cancellationToken);
}

internal sealed class RequestPipeline<TRequest, TResponse> : RequestPipeline<TResponse>
    where TRequest : IRequest<TResponse>
{
    public override Task<TResponse> Handle(
        IRequest<TResponse> request,
        IServiceProvider serviceProvider,
        CancellationToken cancellationToken)
    {
        var typedRequest = (TRequest)request;
        var handler = serviceProvider.GetRequiredService<IRequestHandler<TRequest, TResponse>>();

        RequestHandlerDelegate<TResponse> next = () => handler.Handle(typedRequest, cancellationToken);

        // Registration order == execution order (first registered behavior is the outermost).
        foreach (var behavior in serviceProvider.GetServices<IPipelineBehavior<TRequest, TResponse>>().Reverse())
        {
            var inner = next;
            next = () => behavior.Handle(typedRequest, inner, cancellationToken);
        }

        return next();
    }
}
