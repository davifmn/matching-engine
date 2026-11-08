#!/usr/bin/env bash
# End-to-end check of the drop copy with docker compose (matchd + dropcopy + Kafka):
#   1. trades while everything runs;
#   2. kill -9 of the drop copy, trades while it is down, restart;
#   3. kill -9 of matchd, restart (it replays its journal), more trades.
# Then every trade id the engine reported must be on the topic: none missing and none
# invented (duplicates are allowed: delivery is at least once, consumers deduplicate).
#
# Needs docker compose, curl and jq. Run from the repository root.
set -euo pipefail

URL=${URL:-http://localhost:8080}
COMPOSE=${COMPOSE:-docker compose}
WORK=$(mktemp -d)
EXPECTED="$WORK/expected.txt"
: >"$EXPECTED"

wait_ready() {
  for _ in $(seq 60); do
    curl -fsS "$URL/readyz" >/dev/null 2>&1 && return
    sleep 1
  done
  echo "matchd is not ready"; $COMPOSE logs matchd; exit 1
}

order() { curl -fsS -X POST "$URL/v1/instruments/SAP/orders" -H 'Content-Type: application/json' -d "$1"; }

# cross SELLER BUYER PRICE QTY: a resting sell, then a buy that takes it.
# Records the ids the drop copy must publish: SAP-<seq of the buy>-<n>.
cross() {
  order "{\"side\":\"sell\",\"type\":\"limit\",\"price\":\"$3\",\"quantity\":\"$4\",\"account_id\":$1}" >/dev/null
  order "{\"side\":\"buy\",\"type\":\"limit\",\"time_in_force\":\"ioc\",\"price\":\"$3\",\"quantity\":\"$4\",\"account_id\":$2}" |
    jq -r '.seq as $s | .fills | to_entries[] | "SAP-\($s)-\(.key + 1)"' >>"$EXPECTED"
}

trades_on_topic() {
  $COMPOSE exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic trades.executed --from-beginning --timeout-ms 8000 2>/dev/null || true
}

echo "== start"
$COMPOSE up --build -d
wait_ready

echo "== 1. 40 trades with everything running"
for i in $(seq 1 40); do cross 1001 1002 "134.$((10 + i))" 10; done

echo "== 2. kill -9 the drop copy, 40 trades while it is down, restart it"
$COMPOSE kill -s KILL dropcopy
for i in $(seq 1 40); do cross 1003 1001 "135.$((10 + i))" 5; done
$COMPOSE up -d dropcopy

echo "== 3. kill -9 matchd, restart (journal replay), 20 more trades"
$COMPOSE kill -s KILL matchd
$COMPOSE up -d matchd
wait_ready
for i in $(seq 1 20); do cross 1002 1003 "136.$((10 + i))" 1; done

echo "== check the topic"
want=$(sort -u "$EXPECTED" | wc -l | tr -d ' ')
for attempt in $(seq 1 10); do
  trades_on_topic >"$WORK/topic.json"
  jq -r .trade_id "$WORK/topic.json" | sort >"$WORK/got_all.txt"
  sort -u "$WORK/got_all.txt" >"$WORK/got.txt"
  missing=$(comm -23 <(sort -u "$EXPECTED") "$WORK/got.txt" | wc -l | tr -d ' ')
  [ "$missing" = 0 ] && break
  echo "attempt $attempt: $missing of $want trades not on the topic yet"
  sleep 3
done
invented=$(comm -13 <(sort -u "$EXPECTED") "$WORK/got.txt" | wc -l | tr -d ' ')
total=$(wc -l <"$WORK/got_all.txt" | tr -d ' ')
echo "engine reported $want trades; topic has $total messages, $(wc -l <"$WORK/got.txt" | tr -d ' ') distinct; missing $missing, unexpected $invented"
[ "$missing" = 0 ] || { comm -23 <(sort -u "$EXPECTED") "$WORK/got.txt" | head; $COMPOSE logs dropcopy | tail -50; exit 1; }
[ "$invented" = 0 ] || { echo "the drop copy published trades the engine never reported"; exit 1; }

echo "== a message"
head -1 "$WORK/topic.json" | jq .
head -1 "$WORK/topic.json" | jq -e '.currency == "EUR" and (.buyer_account | test("^[0-9]+$")) and (.seller_account | test("^[0-9]+$")) and .buyer_account != .seller_account' >/dev/null

curl -fsS localhost:9100/metrics | grep '^dropcopy_'
echo "DROP COPY E2E PASSED"
