# syntax=docker/dockerfile:1
# Build context: repository root.  docker build -f src/LedgerFlow.Api/Dockerfile .

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Restore in its own layer: only re-runs when a project file or package version changes.
COPY global.json Directory.Build.props Directory.Packages.props ./
COPY src/LedgerFlow.Domain/LedgerFlow.Domain.csproj src/LedgerFlow.Domain/
COPY src/LedgerFlow.Application/LedgerFlow.Application.csproj src/LedgerFlow.Application/
COPY src/LedgerFlow.Infrastructure/LedgerFlow.Infrastructure.csproj src/LedgerFlow.Infrastructure/
COPY src/LedgerFlow.Migrations.SqlServer/LedgerFlow.Migrations.SqlServer.csproj src/LedgerFlow.Migrations.SqlServer/
COPY src/LedgerFlow.Migrations.Oracle/LedgerFlow.Migrations.Oracle.csproj src/LedgerFlow.Migrations.Oracle/
COPY src/LedgerFlow.Api/LedgerFlow.Api.csproj src/LedgerFlow.Api/
RUN dotnet restore src/LedgerFlow.Api/LedgerFlow.Api.csproj

COPY protos/ protos/
COPY src/ src/
RUN dotnet publish src/LedgerFlow.Api/LedgerFlow.Api.csproj -c Release -o /app --no-restore /p:UseAppHost=false

# Chiseled "extra": distroless Ubuntu (no shell, non-root) plus ICU — SqlClient does not run in globalization-invariant mode.
FROM mcr.microsoft.com/dotnet/aspnet:10.0-noble-chiseled-extra AS runtime
WORKDIR /app
COPY --from=build /app .
EXPOSE 8080
ENTRYPOINT ["dotnet", "LedgerFlow.Api.dll"]
