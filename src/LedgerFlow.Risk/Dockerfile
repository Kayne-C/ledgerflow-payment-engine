# syntax=docker/dockerfile:1
# Build context: repository root.  docker build -f src/LedgerFlow.Risk/Dockerfile .

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Restore in its own layer: only re-runs when a project file or package version changes.
COPY global.json Directory.Build.props Directory.Packages.props ./
COPY src/LedgerFlow.Risk/LedgerFlow.Risk.csproj src/LedgerFlow.Risk/
RUN dotnet restore src/LedgerFlow.Risk/LedgerFlow.Risk.csproj

COPY protos/ protos/
COPY src/ src/
RUN dotnet publish src/LedgerFlow.Risk/LedgerFlow.Risk.csproj -c Release -o /app --no-restore /p:UseAppHost=false

# Plain chiseled (no ICU needed): distroless, no shell, non-root.
FROM mcr.microsoft.com/dotnet/aspnet:10.0-noble-chiseled AS runtime
WORKDIR /app
COPY --from=build /app .
EXPOSE 8080
ENTRYPOINT ["dotnet", "LedgerFlow.Risk.dll"]
