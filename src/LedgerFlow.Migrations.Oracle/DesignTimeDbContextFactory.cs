using LedgerFlow.Infrastructure;
using LedgerFlow.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace LedgerFlow.Migrations.OracleDb;

/// <summary>Used by <c>dotnet ef</c> only; never opens a connection while generating migrations.</summary>
internal sealed class DesignTimeDbContextFactory : IDesignTimeDbContextFactory<LedgerDbContext>
{
    public LedgerDbContext CreateDbContext(string[] args)
    {
        var options = new DbContextOptionsBuilder<LedgerDbContext>();
        options.UseOracle(
            "User Id=ledgerflow;Password=design-time-only;Data Source=localhost:1521/FREEPDB1",
            oracle => oracle.MigrationsAssembly(DependencyInjection.OracleMigrationsAssembly));
        return new LedgerDbContext(options.Options);
    }
}
