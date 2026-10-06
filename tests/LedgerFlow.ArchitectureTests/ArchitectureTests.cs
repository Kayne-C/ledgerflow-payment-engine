using System.Reflection;
using LedgerFlow.Application.Abstractions.Messaging;
using LedgerFlow.Domain.Common;

namespace LedgerFlow.ArchitectureTests;

public sealed class ArchitectureTests
{
    private static readonly Assembly Domain = typeof(EventSourcedAggregate).Assembly;
    private static readonly Assembly Application = typeof(ISender).Assembly;
    private static readonly Assembly Infrastructure = typeof(Infrastructure.DependencyInjection).Assembly;
    private static readonly Assembly Api = typeof(Api.Security.ApiScopes).Assembly;

    private static IEnumerable<string> References(Assembly assembly) => assembly.GetReferencedAssemblies().Select(a => a.Name!);

    [Fact]
    public void Domain_has_no_dependencies() =>
        Assert.DoesNotContain(References(Domain), n => n.StartsWith("LedgerFlow.", StringComparison.Ordinal) || n.StartsWith("Microsoft.EntityFrameworkCore", StringComparison.Ordinal));

    [Fact]
    public void Application_knows_no_transport_or_database_provider()
    {
        string[] forbidden = ["LedgerFlow.Infrastructure", "LedgerFlow.Api", "LedgerFlow.Processor", "LedgerFlow.Risk", "Confluent.Kafka",
            "Grpc", "Microsoft.AspNetCore", "Microsoft.EntityFrameworkCore.SqlServer", "Microsoft.EntityFrameworkCore.Sqlite", "Oracle."];

        Assert.DoesNotContain(References(Application), n => forbidden.Any(f => n.StartsWith(f, StringComparison.Ordinal)));
    }

    [Fact]
    public void Infrastructure_does_not_depend_on_hosts() =>
        Assert.DoesNotContain(References(Infrastructure), n => n is "LedgerFlow.Api" or "LedgerFlow.Processor" or "LedgerFlow.Risk");

    [Fact]
    public void Aggregates_change_state_only_through_events()
    {
        var offenders = Domain.GetTypes()
            .Where(t => typeof(EventSourcedAggregate).IsAssignableFrom(t) && !t.IsAbstract)
            .SelectMany(t => t.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly))
            .Where(p => p.SetMethod?.IsPublic == true)
            .Select(p => $"{p.DeclaringType!.Name}.{p.Name}");

        Assert.Empty(offenders);
    }

    [Fact]
    public void Domain_events_are_immutable_sealed_records()
    {
        var events = Domain.GetTypes().Where(t => typeof(IDomainEvent).IsAssignableFrom(t) && t is { IsInterface: false, IsAbstract: false }).ToList();

        Assert.NotEmpty(events);
        Assert.All(events, e =>
        {
            Assert.True(e.IsSealed, $"{e.Name} must be sealed.");
            Assert.True(e.GetMethod("<Clone>$") is not null, $"{e.Name} must be a record.");
            Assert.All(e.GetProperties(), p => Assert.True(p.SetMethod is null || p.SetMethod.ReturnParameter.GetRequiredCustomModifiers().Any(m => m.Name == "IsExternalInit"), $"{e.Name}.{p.Name} must be init-only."));
        });
    }

    [Fact]
    public void Every_request_has_exactly_one_internal_sealed_handler()
    {
        var types = Application.GetTypes();
        var handlers = types
            .Where(t => !t.IsAbstract && t.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IRequestHandler<,>)))
            .ToList();
        var requests = types.Where(t => !t.IsAbstract && !t.IsInterface && t.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IRequest<>)));

        Assert.All(handlers, h => Assert.True(h.IsSealed && !h.IsPublic, $"{h.Name} must be internal sealed."));
        Assert.All(requests, r => Assert.Single(handlers, h => h.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericArguments()[0] == r)));
    }

    [Fact]
    public void Api_reaches_use_cases_only_through_the_sender() =>
        Assert.DoesNotContain(
            Api.GetTypes().SelectMany(t => t.GetConstructors().SelectMany(c => c.GetParameters())),
            p => p.ParameterType.IsGenericType && p.ParameterType.GetGenericTypeDefinition() == typeof(IRequestHandler<,>));
}
