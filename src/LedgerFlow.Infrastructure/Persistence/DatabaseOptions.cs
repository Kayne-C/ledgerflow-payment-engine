namespace LedgerFlow.Infrastructure.Persistence;

public enum DatabaseProvider
{
    Sqlite,
    SqlServer,
    Oracle,
}

public sealed class DatabaseOptions
{
    public const string Section = "Database";

    public DatabaseProvider Provider { get; set; } = DatabaseProvider.Sqlite;

    public string ConnectionString { get; set; } = "Data Source=ledgerflow.db";

    public bool ApplyMigrationsOnStartup { get; set; }

    public bool SeedDemoData { get; set; }
}
