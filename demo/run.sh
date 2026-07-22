#!/usr/bin/env bash
#
# End-to-end demo: build a Collector with the NATS exporter + receiver, then push
# a trace through OTLP -> our exporter -> NATS/JetStream -> our receiver -> console.
#
# Requires only Go (1.25+) and curl. NATS server, the stream setup helper, and the
# Collector Builder are all run via `go run ...`, so nothing needs pre-installing.

set -euo pipefail
cd "$(dirname "$0")"

export GOTOOLCHAIN=auto
# The generated Collector under _build is its own module; ignore the repo's
# go.work so it (and the go-run helpers) resolve standalone.
export GOWORK=off

NATS_VERSION="v2.14.3"
BUILDER_VERSION="v0.133.0"
NATS_STORE="$(mktemp -d)"
COLLECTOR_LOG="$(mktemp)"
NATS_LOG="$(mktemp)"

COLLECTOR_PID=""
NATS_PID=""

cleanup() {
  [[ -n "$COLLECTOR_PID" ]] && kill "$COLLECTOR_PID" 2>/dev/null || true
  [[ -n "$NATS_PID" ]] && kill "$NATS_PID" 2>/dev/null || true
  wait 2>/dev/null || true
  rm -rf "$NATS_STORE"
}
trap cleanup EXIT

wait_port() {
  for _ in $(seq 1 120); do
    (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null && { exec 3>&- 3<&-; return 0; }
    sleep 0.5
  done
  return 1
}

echo "==> [1/5] Starting NATS server (JetStream) on :4222 ..."
go run "github.com/nats-io/nats-server/v2@${NATS_VERSION}" -js -sd "$NATS_STORE" -p 4222 >"$NATS_LOG" 2>&1 &
NATS_PID=$!
wait_port 4222 || { echo "NATS did not start; log:"; cat "$NATS_LOG"; exit 1; }

echo "==> [2/5] Creating JetStream stream OTEL_SPANS ..."
( cd streamsetup && go run . )

echo "==> [3/5] Building the Collector with ocb (first run compiles a lot) ..."
go run "go.opentelemetry.io/collector/cmd/builder@${BUILDER_VERSION}" --config builder-config.yaml

echo "==> [4/5] Starting the Collector ..."
./_build/otelcol-natsdemo --config config.yaml >"$COLLECTOR_LOG" 2>&1 &
COLLECTOR_PID=$!
wait_port 4318 || { echo "Collector did not start; log:"; cat "$COLLECTOR_LOG"; exit 1; }
sleep 2

echo "==> [5/5] Injecting a trace via OTLP/HTTP ..."
curl -sS -X POST http://127.0.0.1:4318/v1/traces \
  -H 'Content-Type: application/json' --data @trace.json
echo

echo "==> Waiting for it to flow OTLP -> exporter -> JetStream -> receiver -> debug ..."
sleep 4

echo
echo "======================================================================"
echo "Collector debug output (the consume side printing what it read back):"
echo "======================================================================"
if grep -q "demo-span" "$COLLECTOR_LOG"; then
  grep -B1 -A6 "demo-span" "$COLLECTOR_LOG" | head -40
  echo
  echo "SUCCESS: the span made the full round trip through NATS."
else
  echo "Did not observe the span in the debug output. Full Collector log:"
  echo "  $COLLECTOR_LOG"
  tail -40 "$COLLECTOR_LOG"
  exit 1
fi
