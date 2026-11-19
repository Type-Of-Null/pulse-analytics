#!/usr/bin/env bash
# Run the e2e suite against the kind cluster through port-forwards.
set -euo pipefail
pids=()
trap 'kill "${pids[@]}" 2>/dev/null || true' EXIT
for pair in ingest-api:18001 analytics-api:18002 admin-api:18003; do
  kubectl -n pulse port-forward "svc/${pair%%:*}" "${pair##*:}:80" >/dev/null &
  pids+=($!)
done
sleep 3
PULSE_E2E_INGEST_URL=http://localhost:18001 \
PULSE_E2E_ANALYTICS_URL=http://localhost:18002 \
PULSE_E2E_ADMIN_URL=http://localhost:18003 \
  uv run pytest -m e2e tests/e2e "$@"
