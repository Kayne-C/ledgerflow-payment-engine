using LedgerFlow.Domain.Common;

namespace LedgerFlow.Application.Abstractions.Messaging;

#pragma warning disable CA1040 // Marker interfaces are intentional: they carry the response type for dispatch.
public interface IRequest<TResponse>;
#pragma warning restore CA1040

/// <summary>State-changing use case.</summary>
public interface ICommand : IRequest<Result>;

/// <summary>State-changing use case that returns a value (e.g. the id of a created aggregate).</summary>
public interface ICommand<TResponse> : IRequest<Result<TResponse>>;

/// <summary>Side-effect free read model request.</summary>
public interface IQuery<TResponse> : IRequest<Result<TResponse>>;

public interface IRequestHandler<in TRequest, TResponse>
    where TRequest : IRequest<TResponse>
{
    Task<TResponse> Handle(TRequest request, CancellationToken cancellationToken);
}

public interface ICommandHandler<in TCommand> : IRequestHandler<TCommand, Result>
    where TCommand : ICommand;

public interface ICommandHandler<in TCommand, TResponse> : IRequestHandler<TCommand, Result<TResponse>>
    where TCommand : ICommand<TResponse>;

public interface IQueryHandler<in TQuery, TResponse> : IRequestHandler<TQuery, Result<TResponse>>
    where TQuery : IQuery<TResponse>;

public delegate Task<TResponse> RequestHandlerDelegate<TResponse>();

/// <summary>Cross-cutting decorator around every request (validation, logging, tracing...).</summary>
public interface IPipelineBehavior<in TRequest, TResponse>
    where TRequest : IRequest<TResponse>
{
    Task<TResponse> Handle(TRequest request, RequestHandlerDelegate<TResponse> next, CancellationToken cancellationToken);
}

public interface ISender
{
    Task<TResponse> Send<TResponse>(IRequest<TResponse> request, CancellationToken cancellationToken = default);
}
