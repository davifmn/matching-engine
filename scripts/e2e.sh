#!/usr/bin/env bash
# End-to-end check of the real binaries, run by CI and by `make e2e`:
#   1. load for a while, graceful restart (SIGTERM): the book must be byte-identical;
#   2. kill -9 in the middle of the load: the server must recover from its journal;
#   3. a clean restart after that recovery must again give an identical book.
#
# Journals and logs go to $WORK (a new temporary directory by default), which is left in
# place for inspection.
set -euo pipefail

BIN=${BIN:-bin}
DURATION=${DURATION:-10s}
WORK=${WORK:-$(mktemp -d)}
ADDR=${ADDR:-127.0.0.1:18080}
URL=http://$ADDR
PID=
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT
mkdir -p "$WORK"
echo "work directory: $WORK"

start() {
  "$BIN/matchd" -addr "$ADDR" -data "$WORK/data" -instruments config/instruments.json >>"$WORK/server.log" 2>&1 &
  PID=$!
  for _ in $(seq 100); do
    curl -fsS "$URL/readyz" >/dev/null 2>&1 && return
    sleep 0.1
  done
  echo "server did not become ready"
  cat "$WORK/server.log"
  exit 1
}
stop() { kill -TERM "$PID"; wait "$PID"; }
book() { curl -fsS "$URL/v1/instruments/PETR4/book?depth=1000"; }
field() { grep -o "\"$1\":[0-9]*" "$2" | head -1 | cut -d: -f2; }

echo "== $DURATION of load, then a graceful restart"
start
"$BIN/loadgen" -url "$URL" -symbol PETR4 -mid 34.00 -clients 16 -duration "$DURATION"
book >"$WORK/before.json"
stop
start
book >"$WORK/after.json"
cmp "$WORK/before.json" "$WORK/after.json"
echo "identical book after restart: seq $(field seq "$WORK/after.json"), $(field resting_orders "$WORK/after.json") resting orders"

echo "== kill -9 under load"
"$BIN/loadgen" -url "$URL" -symbol PETR4 -mid 34.00 -clients 16 -duration 60s >/dev/null 2>&1 &
LOAD=$!
sleep 3
kill -KILL "$PID"
wait "$PID" || true
kill "$LOAD" 2>/dev/null || true
wait "$LOAD" || true
start
book >"$WORK/recovered.json"
stop
start
book >"$WORK/again.json"
stop
cmp "$WORK/recovered.json" "$WORK/again.json"
echo "recovered after kill -9: seq $(field seq "$WORK/recovered.json"), $(field resting_orders "$WORK/recovered.json") resting orders"
grep -h -e '"journal replayed","instrument":"PETR4"' -e 'torn record' "$WORK/server.log" | tail -4
echo "e2e ok"
