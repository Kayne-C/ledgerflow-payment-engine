# syntax=docker/dockerfile:1
# Build context: repository root.  docker build -f src/LedgerFlow.Processor/Dockerfile .

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Restore in its own layer: only re-runs when a project file or package version changes.
COPY global.json Directory.Build.props Directory.Packages.props ./
COPY src/LedgerFlow.Domain/LedgerFlow.Domain.csproj src/LedgerFlow.Domain/
COPY src/LedgerFlow.Application/LedgerFlow.Application.csproj src/LedgerFlow.Application/
COPY src/LedgerFlow.Infrastructure/LedgerFlow.Infrastructure.csproj src/LedgerFlow.Infrastructure/
COPY src/LedgerFlow.Migrations.SqlServer/LedgerFlow.Migrations.SqlServer.csproj src/LedgerFlow.Migrations.SqlServer/
COPY src/LedgerFlow.Migrations.Oracle/LedgerFlow.Migrations.Oracle.csproj src/LedgerFlow.Migrations.Oracle/
COPY src/LedgerFlow.Processor/LedgerFlow.Processor.csproj src/LedgerFlow.Processor/
RUN dotnet restore src/LedgerFlow.Processor/LedgerFlow.Processor.csproj

COPY protos/ protos/
COPY src/ src/
RUN dotnet publish src/LedgerFlow.Processor/LedgerFlow.Processor.csproj -c Release -o /app --no-restore /p:UseAppHost=false

# Chiseled "extra": distroless Ubuntu (no shell, non-root) plus ICU — SqlClient does not run in globalization-invariant mode.
FROM mcr.microsoft.com/dotnet/runtime:10.0-noble-chiseled-extra AS runtime
WORKDIR /app
COPY --from=build /app .

ENTRYPOINT ["dotnet", "LedgerFlow.Processor.dll"]
