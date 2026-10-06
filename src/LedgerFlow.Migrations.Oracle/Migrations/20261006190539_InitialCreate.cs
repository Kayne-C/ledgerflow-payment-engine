using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace LedgerFlow.Migrations.OracleDb.Migrations
{
    /// <inheritdoc />
    public partial class InitialCreate : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "AccountView",
                columns: table => new
                {
                    AccountId = table.Column<Guid>(type: "RAW(16)", nullable: false),
                    Holder = table.Column<string>(type: "NVARCHAR2(128)", maxLength: 128, nullable: false),
                    Currency = table.Column<string>(type: "NCHAR(3)", fixedLength: true, maxLength: 3, nullable: false),
                    Kind = table.Column<string>(type: "NVARCHAR2(32)", maxLength: 32, nullable: false),
                    Status = table.Column<string>(type: "NVARCHAR2(32)", maxLength: 32, nullable: false),
                    BalanceMinor = table.Column<long>(type: "NUMBER(19)", nullable: false),
                    HeldMinor = table.Column<long>(type: "NUMBER(19)", nullable: false),
                    Version = table.Column<long>(type: "NUMBER(19)", nullable: false),
                    OpenedAtUtc = table.Column<DateTime>(type: "TIMESTAMP(7)", nullable: false),
                    UpdatedAtUtc = table.Column<DateTime>(type: "TIMESTAMP(7)", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_AccountView", x => x.AccountId);
                });

            migrationBuilder.CreateTable(
                name: "Events",
                columns: table => new
                {
                    GlobalPosition = table.Column<long>(type: "NUMBER(19)", nullable: false)
                        .Annotation("Oracle:Identity", "START WITH 1 INCREMENT BY 1"),
                    StreamId = table.Column<string>(type: "NVARCHAR2(100)", maxLength: 100, nullable: false),
                    StreamVersion = table.Column<long>(type: "NUMBER(19)", nullable: false),
                    EventId = table.Column<Guid>(type: "RAW(16)", nullable: false),
                    EventType = table.Column<string>(type: "NVARCHAR2(128)", maxLength: 128, nullable: false),
                    Payload = table.Column<string>(type: "NCLOB", maxLength: 1000000, nullable: false),
                    TraceParent = table.Column<string>(type: "NVARCHAR2(128)", maxLength: 128, nullable: true),
                    RecordedAtUtc = table.Column<DateTime>(type: "TIMESTAMP(7)", nullable: false),
                    PreviousHash = table.Column<string>(type: "NCHAR(64)", fixedLength: true, maxLength: 64, nullable: true),
                    Hash = table.Column<string>(type: "NCHAR(64)", fixedLength: true, maxLength: 64, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Events", x => x.GlobalPosition);
                });

            migrationBuilder.CreateTable(
                name: "LedgerEntries",
                columns: table => new
                {
                    Id = table.Column<long>(type: "NUMBER(19)", nullable: false)
                        .Annotation("Oracle:Identity", "START WITH 1 INCREMENT BY 1"),
                    TransactionId = table.Column<Guid>(type: "RAW(16)", nullable: false),
                    AccountId = table.Column<Guid>(type: "RAW(16)", nullable: false),
                    Direction = table.Column<string>(type: "NVARCHAR2(32)", maxLength: 32, nullable: false),
                    AmountMinor = table.Column<long>(type: "NUMBER(19)", nullable: false),
                    BalanceAfterMinor = table.Column<long>(type: "NUMBER(19)", nullable: false),
                    Currency = table.Column<string>(type: "NCHAR(3)", fixedLength: true, maxLength: 3, nullable: false),
                    PostedAtUtc = table.Column<DateTime>(type: "TIMESTAMP(7)", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_LedgerEntries", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Outbox",
                columns: table => new
                {
                    Id = table.Column<long>(type: "NUMBER(19)", nullable: false)
                        .Annotation("Oracle:Identity", "START WITH 1 INCREMENT BY 1"),
                    Topic = table.Column<string>(type: "NVARCHAR2(128)", maxLength: 128, nullable: false),
                    PartitionKey = table.Column<string>(type: "NVARCHAR2(64)", maxLength: 64, nullable: false),
                    MessageType = table.Column<string>(type: "NVARCHAR2(128)", maxLength: 128, nullable: false),
                    MessageId = table.Column<Guid>(type: "RAW(16)", nullable: false),
                    Payload = table.Column<string>(type: "NCLOB", maxLength: 1000000, nullable: false),
                    TraceParent = table.Column<string>(type: "NVARCHAR2(128)", maxLength: 128, nullable: true),
                    OccurredOnUtc = table.Column<DateTime>(type: "TIMESTAMP(7)", nullable: false),
                    LeaseId = table.Column<Guid>(type: "RAW(16)", nullable: true),
                    LeaseExpiresUtc = table.Column<DateTime>(type: "TIMESTAMP(7)", nullable: true),
                    ProcessedOnUtc = table.Column<DateTime>(type: "TIMESTAMP(7)", nullable: true),
                    Attempts = table.Column<int>(type: "NUMBER(10)", nullable: false),
                    LastError = table.Column<string>(type: "NVARCHAR2(2000)", maxLength: 2000, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Outbox", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "PaymentView",
                columns: table => new
                {
                    PaymentId = table.Column<Guid>(type: "RAW(16)", nullable: false),
                    ClientId = table.Column<string>(type: "NVARCHAR2(64)", maxLength: 64, nullable: false),
                    FromAccountId = table.Column<Guid>(type: "RAW(16)", nullable: false),
                    ToAccountId = table.Column<Guid>(type: "RAW(16)", nullable: false),
                    AmountMinor = table.Column<long>(type: "NUMBER(19)", nullable: false),
                    Currency = table.Column<string>(type: "NCHAR(3)", fixedLength: true, maxLength: 3, nullable: false),
                    Reference = table.Column<string>(type: "NVARCHAR2(140)", maxLength: 140, nullable: true),
                    Status = table.Column<string>(type: "NVARCHAR2(32)", maxLength: 32, nullable: false),
                    RiskScore = table.Column<int>(type: "NUMBER(10)", nullable: true),
                    FailureCode = table.Column<string>(type: "NVARCHAR2(64)", maxLength: 64, nullable: true),
                    FailureReason = table.Column<string>(type: "NVARCHAR2(500)", maxLength: 500, nullable: true),
                    InitiatedAtUtc = table.Column<DateTime>(type: "TIMESTAMP(7)", nullable: false),
                    UpdatedAtUtc = table.Column<DateTime>(type: "TIMESTAMP(7)", nullable: false),
                    FinishedAtUtc = table.Column<DateTime>(type: "TIMESTAMP(7)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_PaymentView", x => x.PaymentId);
                });

            migrationBuilder.CreateTable(
                name: "Snapshots",
                columns: table => new
                {
                    StreamId = table.Column<string>(type: "NVARCHAR2(100)", maxLength: 100, nullable: false),
                    Version = table.Column<long>(type: "NUMBER(19)", nullable: false),
                    Payload = table.Column<string>(type: "NCLOB", maxLength: 1000000, nullable: false),
                    LastHash = table.Column<string>(type: "NCHAR(64)", fixedLength: true, maxLength: 64, nullable: true),
                    LastEventRecordedAtUtc = table.Column<DateTime>(type: "TIMESTAMP(7)", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Snapshots", x => new { x.StreamId, x.Version });
                });

            migrationBuilder.CreateIndex(
                name: "IX_AccountView_Currency_Kind",
                table: "AccountView",
                columns: new[] { "Currency", "Kind" });

            migrationBuilder.CreateIndex(
                name: "IX_Events_EventId",
                table: "Events",
                column: "EventId",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_Events_StreamId_StreamVersion",
                table: "Events",
                columns: new[] { "StreamId", "StreamVersion" },
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_LedgerEntries_AccountId_Id",
                table: "LedgerEntries",
                columns: new[] { "AccountId", "Id" });

            migrationBuilder.CreateIndex(
                name: "IX_LedgerEntries_TransactionId",
                table: "LedgerEntries",
                column: "TransactionId");

            migrationBuilder.CreateIndex(
                name: "IX_Outbox_LeaseId",
                table: "Outbox",
                column: "LeaseId");

            migrationBuilder.CreateIndex(
                name: "IX_Outbox_ProcessedOnUtc_Id",
                table: "Outbox",
                columns: new[] { "ProcessedOnUtc", "Id" });

            migrationBuilder.CreateIndex(
                name: "IX_PaymentView_FromAccountId",
                table: "PaymentView",
                column: "FromAccountId");

            migrationBuilder.CreateIndex(
                name: "IX_PaymentView_Status_UpdatedAtUtc",
                table: "PaymentView",
                columns: new[] { "Status", "UpdatedAtUtc" });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "AccountView");

            migrationBuilder.DropTable(
                name: "Events");

            migrationBuilder.DropTable(
                name: "LedgerEntries");

            migrationBuilder.DropTable(
                name: "Outbox");

            migrationBuilder.DropTable(
                name: "PaymentView");

            migrationBuilder.DropTable(
                name: "Snapshots");
        }
    }
}
