using FluentValidation;
using LedgerFlow.Application.Abstractions.Behaviors;
using LedgerFlow.Application.Abstractions.Messaging;
using LedgerFlow.Application.Messaging;
using Microsoft.Extensions.DependencyInjection;

namespace LedgerFlow.Application;

public static class DependencyInjection
{
    public static IServiceCollection AddApplication(this IServiceCollection services)
    {
        var assembly = typeof(DependencyInjection).Assembly;

        services.AddScoped<ISender, Sender>();

        foreach (var type in assembly.GetTypes().Where(t => t is { IsClass: true, IsAbstract: false }))
        {
            var registeredAsMessageHandler = false;
            foreach (var contract in type.GetInterfaces().Where(i => i.IsGenericType))
            {
                var definition = contract.GetGenericTypeDefinition();
                if (definition == typeof(IRequestHandler<,>))
                {
                    services.AddScoped(contract, type);
                }
                else if (definition == typeof(IMessageHandler<>))
                {
                    if (!registeredAsMessageHandler)
                    {
                        services.AddScoped(type);
                        registeredAsMessageHandler = true;
                    }

                    services.AddSingleton(new MessageSubscription(contract.GetGenericArguments()[0], type));
                }
            }
        }

        services.AddSingleton<MessageCatalog>();

        services.AddScoped(typeof(IPipelineBehavior<,>), typeof(RequestTelemetryBehavior<,>));
        services.AddScoped(typeof(IPipelineBehavior<,>), typeof(ValidationBehavior<,>));
        services.AddValidatorsFromAssembly(assembly, includeInternalTypes: true);

        return services;
    }
}
