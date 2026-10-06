#!/usr/bin/env bash
# End-to-end saga throughput and latency: runs payments.js, waits until every payment is terminal,
# then reads latency percentiles straight from the read model (SQL Server stack).
#   tests/load/measure-e2e.sh 500 60s
set -euo pipefail
RATE=${1:-500}; DURATION=${2:-60s}; BASE=${BASE_URL:-http://localhost:8090}; KEY=${API_KEY:-lf_demo_ops_4d8e1b6a0c}
K6=${K6:-k6}
SQL="docker compose exec -T sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P ${MSSQL_SA_PASSWORD:-LedgerFlow_Dev_2026!} -C -d LedgerFlow -h -1 -W -Q"

start=$(date -u +%Y-%m-%dT%H:%M:%S)
# A crossed latency threshold is reported, not fatal: we still want the end-to-end numbers.
$K6 run -q -e BASE_URL="$BASE" -e RATE="$RATE" -e DURATION="$DURATION" "$(dirname "$0")/payments.js" || true

echo "Waiting for the saga to drain..."
while true; do
  inflight=$(curl -s -H "X-Api-Key: $KEY" "$BASE/api/v1/admin/reconciliation" | python3 -c "import sys,json; print(json.load(sys.stdin)['paymentsInFlight'])")
  [ "$inflight" = "0" ] && break
  sleep 1
done

$SQL "SET NOCOUNT ON;
SELECT Status, COUNT(*) FROM PaymentView WHERE InitiatedAtUtc >= '$start' GROUP BY Status;
WITH d AS (SELECT DATEDIFF(millisecond, InitiatedAtUtc, FinishedAtUtc) AS ms FROM PaymentView WHERE InitiatedAtUtc >= '$start' AND FinishedAtUtc IS NOT NULL)
SELECT DISTINCT 'saga latency ms p50/p95/p99',
  PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ms) OVER (),
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ms) OVER (),
  PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY ms) OVER () FROM d;
SELECT 'e2e throughput payments/s', CAST(COUNT(*) * 1000.0 / NULLIF(DATEDIFF(millisecond, MIN(InitiatedAtUtc), MAX(FinishedAtUtc)), 0) AS DECIMAL(10,1))
FROM PaymentView WHERE InitiatedAtUtc >= '$start';"

curl -s -H "X-Api-Key: $KEY" "$BASE/api/v1/admin/reconciliation" | python3 -c "import sys,json; r=json.load(sys.stdin); print('reconciliation clean:', r['isClean'], '| accounts', r['accountsChecked'], '| journal lines', r['ledgerEntriesChecked'], '| issues', len(r['issues']))"
