using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace LedgerFlow.Migrations.SqlServer.Migrations
{
    /// <summary>
    /// SQL Server-only tuning found under load (Oracle already behaves this way by default):
    /// <list type="bullet">
    /// <item>READ_COMMITTED_SNAPSHOT: readers (stream loads, outbox polling, read models) see the last committed
    /// version instead of waiting for writers. Expected-version checks still happen on write via the unique index.</item>
    /// <item>OPTIMIZE_FOR_SEQUENTIAL_KEY: inserts into IDENTITY-clustered tables all land on the last page and convoy
    /// on PAGELATCH_EX; SQL Server 2019+ throttles that convoy.</item>
    /// <item>Async statistics updates: fast-growing tables no longer block queries on WAIT_ON_SYNC_STATISTICS_REFRESH.</item>
    /// </list>
    /// </summary>
    public partial class SequentialKeyInsertTuning : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.Sql("ALTER INDEX [PK_Events] ON [Events] SET (OPTIMIZE_FOR_SEQUENTIAL_KEY = ON);");
            migrationBuilder.Sql("ALTER INDEX [PK_Outbox] ON [Outbox] SET (OPTIMIZE_FOR_SEQUENTIAL_KEY = ON);");
            migrationBuilder.Sql("ALTER INDEX [PK_LedgerEntries] ON [LedgerEntries] SET (OPTIMIZE_FOR_SEQUENTIAL_KEY = ON);");
            migrationBuilder.Sql("ALTER DATABASE CURRENT SET AUTO_UPDATE_STATISTICS_ASYNC ON;", suppressTransaction: true);
            migrationBuilder.Sql("ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE;", suppressTransaction: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.Sql("ALTER INDEX [PK_Events] ON [Events] SET (OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF);");
            migrationBuilder.Sql("ALTER INDEX [PK_Outbox] ON [Outbox] SET (OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF);");
            migrationBuilder.Sql("ALTER INDEX [PK_LedgerEntries] ON [LedgerEntries] SET (OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF);");
            migrationBuilder.Sql("ALTER DATABASE CURRENT SET AUTO_UPDATE_STATISTICS_ASYNC OFF;", suppressTransaction: true);
            migrationBuilder.Sql("ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT OFF WITH ROLLBACK IMMEDIATE;", suppressTransaction: true);
        }
    }
}
