#!/usr/bin/env bash
# Local end-to-end demo of the OTA rollout orchestrator.
#
# Scenario A (happy path): 6 devices, two batches [2,4], ratio 0.8 -> both
# gates pass and every device converges to v2.0.
# Scenario B (failure isolation + rollback): 4 devices, ratio 1.0, one device
# reports a failed install -> the gate fails, the whole batch rolls back to
# its captured baseline, later batches never start.
#
# Usage: ./scripts/demo.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ADDR="http://127.0.0.1:18099"
STATE="$(mktemp -d)/state.json"
export OTAD_ADDR="$ADDR"

go -C "$ROOT" build -o /tmp/otad-demo ./cmd/otad
go -C "$ROOT" build -o /tmp/otactl-demo ./cmd/otactl
ctl() { /tmp/otactl-demo "$@"; }

/tmp/otad-demo -addr :18099 -state "$STATE" -batch-wait 600ms -poll 80ms \
  > /tmp/otad-demo.log 2>&1 &
PID=$!
trap 'kill -INT $PID 2>/dev/null || true' EXIT
sleep 0.6

echo "################ Scenario A: healthy canary 2/4 ################"
for i in 0 1 2 3 4 5; do ctl device add "a$i" v1.0 >/dev/null; done
ctl create canary v2.0 --devices a0,a1,a2,a3,a4,a5 --sizes 2,4 --ratio 0.8 >/dev/null
sleep 2
ctl get canary | python3 -c '
import json,sys
r=json.load(sys.stdin)
print("phase:", r["phase"], "-", r["reason"])
for b in r["batches"]:
    print("  batch %(index)d: %(state)s success=%(success)d required=%(required)d" % b)
'

echo
echo "################ Scenario B: failed install -> rollback ################"
for i in 0 1 2 3; do ctl device add "b$i" v1.0 >/dev/null; done
# Deterministic fault injection BEFORE create: b0's install is guaranteed to
# fail, so the ratio=1.0 gate cannot pass.
ctl fault b0 INSTALL_FAIL >/dev/null
ctl create bad v2.0 --devices b0,b1,b2,b3 --sizes 2,2 --ratio 1.0 >/dev/null
sleep 2
ctl get bad | python3 -c '
import json,sys
r=json.load(sys.stdin)
print("phase:", r["phase"], "-", r["reason"])
for b in r["batches"]:
    print("  batch %(index)d: %(state)s success=%(success)d failed=%(failed)d offline=%(offline)d required=%(required)d" % b)
'
echo "-- per-device convergence (identical target => deterministic result) --"
ctl participants bad | python3 -c '
import json,sys
for p in json.load(sys.stdin):
    print("  %(device_id)s: %(state)s | %(reason)s" % p)
'
echo
echo "daemon decision log: /tmp/otad-demo.log"
