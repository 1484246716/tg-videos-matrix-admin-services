#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${API_BASE_URL:-http://localhost:3001/api}"
ADMIN_JWT="${ADMIN_JWT:-}"
TARGET_INDEX="${TARGET_INDEX:-search_documents_v2}"
OUTBOX_BATCH_LIMIT="${OUTBOX_BATCH_LIMIT:-200}"
OUTBOX_MAX_ROUNDS="${OUTBOX_MAX_ROUNDS:-120}"
SKIP_FULL_REBUILD="${SKIP_FULL_REBUILD:-false}"

if [[ -z "$ADMIN_JWT" ]]; then
  echo "[ERROR] ADMIN_JWT is required."
  echo "Usage: ADMIN_JWT='<jwt>' ./scripts/search-reindex-oneclick.sh"
  exit 1
fi

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "[ERROR] command not found: $1"
    exit 1
  fi
}

require_cmd curl
require_cmd pnpm
require_cmd npx

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APPS_DIR="$REPO_ROOT/apps"
if [[ ! -d "$APPS_DIR" ]]; then
  echo "[ERROR] apps directory not found: $APPS_DIR"
  exit 1
fi

json_get() {
  local expr="$1"
  if command -v jq >/dev/null 2>&1; then
    jq -r "$expr"
  else
    python3 -c "import json,sys; d=json.load(sys.stdin); print($expr)"
  fi
}

api_get() {
  local url="$1"
  curl -sS --fail \
    -H "Authorization: Bearer $ADMIN_JWT" \
    "$url"
}

api_post() {
  local url="$1"
  local body="${2:-}"
  if [[ -z "$body" ]]; then
    curl -sS --fail -X POST \
      -H "Authorization: Bearer $ADMIN_JWT" \
      "$url"
  else
    curl -sS --fail -X POST \
      -H "Authorization: Bearer $ADMIN_JWT" \
      -H "Content-Type: application/json" \
      -d "$body" \
      "$url"
  fi
}

section() {
  echo
  echo "========== $1 =========="
}

get_pending_outbox() {
  local stats_json="$1"
  if command -v jq >/dev/null 2>&1; then
    echo "$stats_json" | jq '[.outbox[]? | select(.status=="pending" or .status=="processing") | .count] | add // 0'
  else
    python3 - <<'PY'
import json,sys
stats=json.load(sys.stdin)
print(sum(item.get('count',0) for item in stats.get('outbox',[]) if item.get('status') in ('pending','processing')))
PY
  fi
}

STARTED_AT="$(date -Iseconds)"

section "0) Collect baseline stats"
BEFORE_STATS="$(api_get "$API_BASE_URL/search/stats")"
echo "$BEFORE_STATS" | (command -v jq >/dev/null 2>&1 && jq . || cat)

section "1) Init OpenSearch index and aliases"
INIT_RESP="$(api_post "$API_BASE_URL/search/opensearch/init")"
echo "$INIT_RESP" | (command -v jq >/dev/null 2>&1 && jq . || cat)

if [[ "$SKIP_FULL_REBUILD" != "true" ]]; then
  section "2) Full rebuild search_documents table"
  pushd "$APPS_DIR" >/dev/null
  DOTENV_CONFIG_PATH="$APPS_DIR/.env" pnpm --filter @tg-crm/worker exec ts-node -r dotenv/config scripts/build-search-index-full.ts
  popd >/dev/null
else
  section "2) Skip full rebuild"
fi

section "3) Process outbox in loop"
OUTBOX_PROCESSED_TOTAL=0
OUTBOX_SUCCESS_TOTAL=0
OUTBOX_FAILED_TOTAL=0
OUTBOX_ROUNDS=0

for ((i=1; i<=OUTBOX_MAX_ROUNDS; i++)); do
  RESP="$(api_post "$API_BASE_URL/search/outbox/process?limit=$OUTBOX_BATCH_LIMIT")"
  OUTBOX_ROUNDS=$i

  if command -v jq >/dev/null 2>&1; then
    PROCESSED=$(echo "$RESP" | jq -r '.processed // 0')
    SUCCESS=$(echo "$RESP" | jq -r '.success // 0')
    FAILED=$(echo "$RESP" | jq -r '.failed // 0')
  else
    PROCESSED=$(python3 -c 'import json,sys; print(json.load(sys.stdin).get("processed",0))' <<< "$RESP")
    SUCCESS=$(python3 -c 'import json,sys; print(json.load(sys.stdin).get("success",0))' <<< "$RESP")
    FAILED=$(python3 -c 'import json,sys; print(json.load(sys.stdin).get("failed",0))' <<< "$RESP")
  fi

  OUTBOX_PROCESSED_TOTAL=$((OUTBOX_PROCESSED_TOTAL + PROCESSED))
  OUTBOX_SUCCESS_TOTAL=$((OUTBOX_SUCCESS_TOTAL + SUCCESS))
  OUTBOX_FAILED_TOTAL=$((OUTBOX_FAILED_TOTAL + FAILED))

  STATS="$(api_get "$API_BASE_URL/search/stats")"
  PENDING="$(get_pending_outbox "$STATS")"

  echo "Round #$i: processed=$PROCESSED, success=$SUCCESS, failed=$FAILED, pending/processing=$PENDING"

  if [[ "$PENDING" -le 0 ]]; then
    echo "Outbox is empty."
    break
  fi
  sleep 1
done

section "4) Switch aliases to target index"
SWITCH_RESP="$(api_post "$API_BASE_URL/search/opensearch/switch" "{\"targetIndex\":\"$TARGET_INDEX\"}")"
echo "$SWITCH_RESP" | (command -v jq >/dev/null 2>&1 && jq . || cat)

section "5) Final acceptance stats"
AFTER_STATS="$(api_get "$API_BASE_URL/search/stats")"
echo "$AFTER_STATS" | (command -v jq >/dev/null 2>&1 && jq . || cat)

PENDING_FINAL="$(get_pending_outbox "$AFTER_STATS")"

FINISHED_AT="$(date -Iseconds)"

section "Summary"
cat <<EOF
startedAt: $STARTED_AT
finishedAt: $FINISHED_AT
apiBaseUrl: $API_BASE_URL
targetIndex: $TARGET_INDEX
fullRebuildExecuted: $([[ "$SKIP_FULL_REBUILD" == "true" ]] && echo false || echo true)
outboxProcessRounds: $OUTBOX_ROUNDS
outboxProcessedTotal: $OUTBOX_PROCESSED_TOTAL
outboxSuccessTotal: $OUTBOX_SUCCESS_TOTAL
outboxFailedTotal: $OUTBOX_FAILED_TOTAL
finalPendingOutbox: $PENDING_FINAL
EOF

echo
echo "Done: one-click flow finished."
