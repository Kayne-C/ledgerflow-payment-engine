using System.Text.Json;
using System.Text.Json.Serialization;
using LedgerFlow.Domain.Common;

namespace LedgerFlow.Infrastructure.EventStore;

/// <summary>
/// Maps stored type names to the closed set of domain event types. Unknown names fail loudly instead of
/// loading arbitrary types from strings found in the database.
/// </summary>
public static class EventSerializer
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() },
    };

    private static readonly Dictionary<string, Type> TypesByName = typeof(IDomainEvent).Assembly.GetTypes()
        .Where(t => t is { IsClass: true, IsAbstract: false } && typeof(IDomainEvent).IsAssignableFrom(t))
        .ToDictionary(t => t.Name, StringComparer.Ordinal);

    public static string TypeName(IDomainEvent domainEvent) => domainEvent.GetType().Name;

    public static string Serialize(IDomainEvent domainEvent) => JsonSerializer.Serialize(domainEvent, domainEvent.GetType(), Options);

    public static IDomainEvent Deserialize(string eventType, string payload) =>
        TypesByName.TryGetValue(eventType, out var type)
            ? (IDomainEvent)(JsonSerializer.Deserialize(payload, type, Options) ?? throw new InvalidOperationException($"Empty {eventType} payload."))
            : throw new InvalidOperationException($"Unknown event type '{eventType}'.");
}
