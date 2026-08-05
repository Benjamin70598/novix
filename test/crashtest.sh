#!/usr/bin/env bash
# Crash test for milestone 1.
#
# Contract being verified:
#   1. Every order posgen received an ACK for exists in the log after any
#      number of kill -9 restarts of novixd.        (zero acked loss)
#   2. site_seq is contiguous 1..max with no gaps.  (log integrity)
#   3. A consumer can replay the full log cleanly.  (kds -until)
#
# Duplicate business events (ack lost after commit, producer retried) are
# at-least-once behavior: counted and reported, not a failure.
set -u

DIR="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$DIR/bin"
WORK="$(mktemp -d)"
PORT="${PORT:-9410}"
URL="http://127.0.0.1:$PORT"
COUNT="${COUNT:-2000}"
RATE="${RATE:-300}"
KILL_EVERY="${KILL_EVERY:-2}"   # seconds between kill -9 cycles

cleanup() {
  kill -9 "${NOVIXD_PID:-0}" 2>/dev/null
  kill "${KILLER_PID:-0}" 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

start_novixd() {
  "$BIN/novixd" -site store-a -data-dir "$WORK/data" -listen "127.0.0.1:$PORT" \
    >>"$WORK/novixd.log" 2>&1 &
  NOVIXD_PID=$!
}

echo "== crash test: $COUNT orders at $RATE/s, kill -9 every ${KILL_EVERY}s =="
echo "   work dir: $WORK"

start_novixd
sleep 0.5

# The killer: kill -9 novixd on a loop, restart it after a short gap.
(
  while true; do
    sleep "$KILL_EVERY"
    kill -9 "$NOVIXD_PID" 2>/dev/null
    echo "  [killer] kill -9 sent"
    sleep 0.7   # leave it dead long enough that producers actually see it
    "$BIN/novixd" -site store-a -data-dir "$WORK/data" -listen "127.0.0.1:$PORT" \
      >>"$WORK/novixd.log" 2>&1 &
    NOVIXD_PID=$!
  done
) &
KILLER_PID=$!

# Producer runs to completion through the carnage.
"$BIN/posgen" -novixd "$URL" -count "$COUNT" -rate "$RATE" \
  -ledger "$WORK/acked.log" -site store-a

# Stop the killer, make sure novixd is up for verification.
kill "$KILLER_PID" 2>/dev/null
wait "$KILLER_PID" 2>/dev/null
sleep 1
if ! curl -sf "$URL/v1/status" >/dev/null; then
  start_novixd
  sleep 0.5
fi

echo "== verifying =="

MAX=$(curl -sf "$URL/v1/status" | python3 -c 'import sys,json; print(json.load(sys.stdin)["topics"].get("orders",0))')
ACKED=$(wc -l < "$WORK/acked.log")
echo "   acked by producer : $ACKED"
echo "   stored in log     : $MAX"

# Pull the full log and verify: contiguity + every acked id present.
python3 - "$URL" "$MAX" "$WORK/acked.log" <<'EOF'
import base64, json, sys, urllib.request

url, maxseq, ledger = sys.argv[1], int(sys.argv[2]), sys.argv[3]
seqs, ids = [], []
after = 0
while True:
    with urllib.request.urlopen(f"{url}/v1/topics/orders/events?after={after}&limit=1000") as r:
        batch = json.load(r)["events"]
    if not batch:
        break
    for e in batch:
        seqs.append(e["site_seq"])
        p = e["payload"]
        # Go []byte marshals to a base64 JSON string.
        doc = json.loads(base64.b64decode(p)) if isinstance(p, str) else p
        ids.append(doc["order_id"])
    after = batch[-1]["site_seq"]

# contiguity
expected = list(range(1, maxseq + 1))
assert seqs == expected, f"contiguity violated: {len(seqs)} events, max {maxseq}"

acked = [line.strip() for line in open(ledger) if line.strip()]
stored = set(ids)
missing = [a for a in acked if a not in stored]
assert not missing, f"ACKED EVENTS LOST: {missing[:5]} ({len(missing)} total)"

from collections import Counter
dups = {k: v for k, v in Counter(ids).items() if v > 1}
print(f"   contiguity        : OK (1..{maxseq}, no gaps)")
print(f"   acked loss        : 0 of {len(acked)}")
print(f"   at-least-once dups: {len(dups)} (allowed, produced by retry after lost ack)")
EOF
VERIFY=$?
if [ $VERIFY -ne 0 ]; then
  echo "== CRASH TEST FAILED =="
  exit 1
fi

# Full replay through the consumer path.
"$BIN/kds" -novixd "$URL" -name crashtest-replay -until "$MAX" -quiet
if [ $? -ne 0 ]; then
  echo "== CRASH TEST FAILED (consumer replay) =="
  exit 1
fi

KILLS=$(grep -c 'kill -9 sent' /dev/stdin 2>/dev/null <<< "" ; true)
echo "== CRASH TEST PASSED =="
