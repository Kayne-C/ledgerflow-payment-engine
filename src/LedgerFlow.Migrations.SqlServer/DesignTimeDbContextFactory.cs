using LedgerFlow.Infrastructure;
using LedgerFlow.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace LedgerFlow.Migrations.SqlServer;

/// <summary>Used by <c>dotnet ef</c> only; never opens a connection while generating migrations.</summary>
internal sealed class DesignTimeDbContextFactory : IDesignTimeDbContextFactory<LedgerDbContext>
{
    public LedgerDbContext CreateDbContext(string[] args)
    {
        var options = new DbContextOptionsBuilder<LedgerDbContext>();
        options.UseSqlServer(
            "Server=localhost,1433;Database=LedgerFlow;User Id=sa;Password=Design-time-only1;TrustServerCertificate=True",
            sql => sql.MigrationsAssembly(DependencyInjection.SqlServerMigrationsAssembly));
        return new LedgerDbContext(options.Options);
    }
}
