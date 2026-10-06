using LedgerFlow.Application.ReadModels;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Oracle.ManagedDataAccess.Client;

namespace LedgerFlow.Infrastructure.Persistence;

public sealed class LedgerDbContext(DbContextOptions<LedgerDbContext> options) : DbContext(options)
{
    public DbSet<EventRecord> Events => Set<EventRecord>();

    public DbSet<SnapshotRecord> Snapshots => Set<SnapshotRecord>();

    public DbSet<OutboxRecord> Outbox => Set<OutboxRecord>();

    public DbSet<AccountView> AccountViews => Set<AccountView>();

    public DbSet<LedgerEntryView> LedgerEntries => Set<LedgerEntryView>();

    public DbSet<PaymentView> PaymentViews => Set<PaymentView>();

    /// <summary>True when the write failed because another writer got there first (retry with fresh state).</summary>
    public static bool IsConcurrencyFailure(DbUpdateException exception) => exception.InnerException switch
    {
        SqlException sql => sql.Number is 2601 or 2627 or 1205,     // unique index, unique constraint, deadlock victim
        OracleException oracle => oracle.Number is 1 or 60,         // ORA-00001 unique, ORA-00060 deadlock
        SqliteException sqlite => sqlite.SqliteErrorCode == 19 && sqlite.SqliteExtendedErrorCode is 2067 or 1555,
        _ => false,
    };

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<EventRecord>(e =>
        {
            e.ToTable("Events");
            e.HasKey(x => x.GlobalPosition);
            e.Property(x => x.GlobalPosition).ValueGeneratedOnAdd();
            e.Property(x => x.StreamId).HasMaxLength(EventRecord.StreamIdMaxLength).IsRequired();
            e.Property(x => x.EventType).HasMaxLength(EventRecord.TypeMaxLength).IsRequired();
            e.Property(x => x.Payload).HasMaxLength(EventRecord.PayloadMaxLength).IsRequired();
            e.Property(x => x.TraceParent).HasMaxLength(128);
            e.Property(x => x.PreviousHash).HasMaxLength(EventRecord.HashLength).IsFixedLength();
            e.Property(x => x.Hash).HasMaxLength(EventRecord.HashLength).IsFixedLength().IsRequired();
            e.HasIndex(x => new { x.StreamId, x.StreamVersion }).IsUnique();
            e.HasIndex(x => x.EventId).IsUnique();
        });

        modelBuilder.Entity<SnapshotRecord>(e =>
        {
            e.ToTable("Snapshots");
            e.HasKey(x => new { x.StreamId, x.Version });
            e.Property(x => x.StreamId).HasMaxLength(EventRecord.StreamIdMaxLength);
            e.Property(x => x.Payload).HasMaxLength(EventRecord.PayloadMaxLength).IsRequired();
            e.Property(x => x.LastHash).HasMaxLength(EventRecord.HashLength).IsFixedLength();
        });

        modelBuilder.Entity<OutboxRecord>(e =>
        {
            e.ToTable("Outbox");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).ValueGeneratedOnAdd();
            e.Property(x => x.Topic).HasMaxLength(OutboxRecord.TopicMaxLength).IsRequired();
            e.Property(x => x.PartitionKey).HasMaxLength(64).IsRequired();
            e.Property(x => x.MessageType).HasMaxLength(EventRecord.TypeMaxLength).IsRequired();
            e.Property(x => x.Payload).HasMaxLength(EventRecord.PayloadMaxLength).IsRequired();
            e.Property(x => x.TraceParent).HasMaxLength(128);
            e.Property(x => x.LastError).HasMaxLength(OutboxRecord.ErrorMaxLength);
            e.HasIndex(x => new { x.ProcessedOnUtc, x.Id });
            e.HasIndex(x => x.LeaseId);
        });

        modelBuilder.Entity<AccountView>(e =>
        {
            e.ToTable("AccountView");
            e.HasKey(x => x.AccountId);
            e.Property(x => x.AccountId).ValueGeneratedNever();
            e.Property(x => x.Holder).HasMaxLength(128).IsRequired();
            e.Property(x => x.Currency).HasMaxLength(3).IsFixedLength().IsRequired();
            e.HasIndex(x => new { x.Currency, x.Kind });
        });

        modelBuilder.Entity<LedgerEntryView>(e =>
        {
            e.ToTable("LedgerEntries");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).ValueGeneratedOnAdd();
            e.Property(x => x.Currency).HasMaxLength(3).IsFixedLength().IsRequired();
            e.HasIndex(x => new { x.AccountId, x.Id });
            e.HasIndex(x => x.TransactionId);
        });

        modelBuilder.Entity<PaymentView>(e =>
        {
            e.ToTable("PaymentView");
            e.HasKey(x => x.PaymentId);
            e.Property(x => x.PaymentId).ValueGeneratedNever();
            e.Property(x => x.ClientId).HasMaxLength(64).IsRequired();
            e.Property(x => x.Currency).HasMaxLength(3).IsFixedLength().IsRequired();
            e.Property(x => x.Reference).HasMaxLength(140);
            e.Property(x => x.FailureCode).HasMaxLength(64);
            e.Property(x => x.FailureReason).HasMaxLength(500);
            e.HasIndex(x => new { x.Status, x.UpdatedAtUtc });
            e.HasIndex(x => x.FromAccountId);
        });
    }

    protected override void ConfigureConventions(ModelConfigurationBuilder configurationBuilder)
    {
        // Readable in SQL tools and stable across enum reordering.
        configurationBuilder.Properties<Enum>().HaveConversion<string>().HaveMaxLength(32);
    }
}
