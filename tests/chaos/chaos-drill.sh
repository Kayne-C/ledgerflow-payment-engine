#!/usr/bin/env bash
# Chaos drill against the docker-compose stack (SQL Server):
#   sustained payment load while (1) a processor replica is SIGKILLed, (2) the risk engine is down for 20 s,
#   (3) Kafka is restarted. Afterwards every payment must be terminal and the ledger must reconcile to the cent.
#
#   tests/chaos/chaos-drill.sh 30 100s
#
# ACCOUNTS spreads the load: the risk engine's velocity rule rejects more than 30 payments per account per minute,
# so 30/s across 50 accounts would (correctly) be partly rejected. 200 accounts keeps the drill about outages.
set -uo pipefail
RATE=${1:-30}; DURATION=${2:-100s}; BASE=${BASE_URL:-http://localhost:8090}; KEY=${API_KEY:-lf_demo_ops_4d8e1b6a0c}
K6=${K6:-k6}; DRAIN_TIMEOUT=${DRAIN_TIMEOUT:-600}; ACCOUNTS=${ACCOUNTS:-200}
cd "$(dirname "$0")/../.."
SQL="docker compose exec -T sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P ${MSSQL_SA_PASSWORD:-LedgerFlow_Dev_2026!} -C -d LedgerFlow -h -1 -W -Q"
log() { echo "[$(date -u +%H:%M:%S)] $*"; }

start=$(date -u +%Y-%m-%dT%H:%M:%S)
$K6 run -q -e BASE_URL="$BASE" -e RATE="$RATE" -e DURATION="$DURATION" -e ACCOUNTS="$ACCOUNTS" tests/load/payments.js > /tmp/chaos-k6.log 2>&1 &
k6_pid=$!

# k6 first opens and funds the accounts; start the clock once payments are actually flowing.
until [ "$($SQL "SET NOCOUNT ON; SELECT COUNT(*) FROM PaymentView WHERE InitiatedAtUtc >= '$start'" 2>/dev/null | tr -d '[:space:]')" -gt 0 ] 2>/dev/null; do sleep 1; done
log "payments flowing"

sleep 20; victim=$(docker compose ps -q processor | head -1); log "SIGKILL processor $victim"; docker kill "$victim" > /dev/null
sleep 15; log "risk engine down"; docker compose stop risk > /dev/null 2>&1
sleep 20; log "risk engine up"; docker compose start risk > /dev/null 2>&1
sleep 10; log "processor replica back"; docker start "$victim" > /dev/null
sleep 10; log "restart kafka"; docker compose restart kafka > /dev/null 2>&1

wait $k6_pid
grep -E "http_reqs\.|http_req_failed\." /tmp/chaos-k6.log

log "load finished; waiting for every payment to reach a terminal state (sweeper re-drives dead-lettered steps)"
for ((i = 0; i < DRAIN_TIMEOUT; i += 5)); do
  inflight=$(curl -s -H "X-Api-Key: $KEY" "$BASE/api/v1/admin/reconciliation" | python3 -c "import sys,json; print(json.load(sys.stdin)['paymentsInFlight'])" 2>/dev/null || echo "?")
  [ "$inflight" = "0" ] && break
  sleep 5
done
log "in flight: $inflight (after ${i}s)"

$SQL "SET NOCOUNT ON;
SELECT Status, COUNT(*) FROM PaymentView WHERE InitiatedAtUtc >= '$start' GROUP BY Status;
SELECT 'failure reasons', FailureCode, COUNT(*) FROM PaymentView WHERE InitiatedAtUtc >= '$start' AND Status = 'Failed' GROUP BY FailureCode;
SELECT 'duplicate settlements', COUNT(*) FROM (SELECT TransactionId FROM LedgerEntries GROUP BY TransactionId HAVING COUNT(*) <> 2) d;"

docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic ledgerflow.payment-commands.dlq 2>/dev/null \
  | awk -F: '{ sum += $3 } END { print "dead-lettered messages:", sum + 0 }'

curl -s -H "X-Api-Key: $KEY" "$BASE/api/v1/admin/reconciliation" | python3 -c "
import sys, json
r = json.load(sys.stdin)
print('reconciliation clean:', r['isClean'], '| accounts', r['accountsChecked'], '| journal lines', r['ledgerEntriesChecked'], '| in flight', r['paymentsInFlight'], '| issues', r['issues'][:5])"
curl -s -H "X-Api-Key: $KEY" "$BASE/api/v1/admin/trial-balance"; echo
