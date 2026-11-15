#!/usr/bin/env bash
# The failover demo, end to end, against the docker compose cluster:
#
#   1. start five nodes            4. watch a new leader get elected
#   2. write thousands of records  5. bring the old leader back
#   3. kill the leader (SIGKILL)   6. verify every node converged
#
# Requires docker compose and a release build of the CLI (built if missing).
set -euo pipefail
cd "$(dirname "$0")/.."

CLI=./target/release/ironclad-cli
[[ -x $CLI ]] || cargo build --release --bin ironclad-cli
ADDRS=127.0.0.1:6001,127.0.0.1:6002,127.0.0.1:6003,127.0.0.1:6004,127.0.0.1:6005
RECORDS=${RECORDS:-5000}

field() { grep "^$2=" <<<"$1" | cut -d= -f2; }
node_status() { "$CLI" -a "127.0.0.1:$((6000 + $1))" status 2>/dev/null | tail -n +2; }
leader() {
  for i in 1 2 3 4 5; do
    s=$(node_status "$i") || continue
    [[ $(field "$s" role) == leader ]] && { echo "$i $(field "$s" term)"; return; }
  done
  return 1
}
wait_leader() {
  for _ in $(seq 100); do leader && return; sleep 0.2; done
  echo "no leader elected" >&2; exit 1
}

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

step "1. starting a fresh five-node cluster"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose up -d --build
read -r OLD TERM < <(wait_leader)
echo "leader: node$OLD (term $TERM)"

step "2. writing $RECORDS records"
"$CLI" -a "$ADDRS" bench --ops "$RECORDS" --concurrency 32 --keys "$RECORDS" | grep -E 'operations|throughput|p50|p99 '
"$CLI" -a "$ADDRS" set demo:marker before-failover

step "3. killing the leader (node$OLD) with SIGKILL"
docker compose kill -s SIGKILL "node$OLD"
START=$(date +%s%N)

step "4. waiting for a new leader"
read -r NEW NEW_TERM < <(wait_leader)
echo "new leader: node$NEW (term $NEW_TERM) after $(( ($(date +%s%N) - START) / 1000000 )) ms"
"$CLI" -a "$ADDRS" set demo:marker after-failover
"$CLI" -a "$ADDRS" bench --ops 1000 --concurrency 16 | grep -E 'operations'

step "5. restarting node$OLD with its old data"
docker compose start "node$OLD"

step "6. verifying convergence"
for _ in $(seq 150); do
  idx=()
  for i in 1 2 3 4 5; do idx+=("$(field "$(node_status "$i" || true)" applied_index)"); done
  echo "applied_index per node: ${idx[*]}"
  if [[ -n ${idx[0]} && $(printf '%s\n' "${idx[@]}" | sort -u | wc -l) -eq 1 ]]; then
    for i in 1 2 3 4 5; do
      v=$("$CLI" -a "127.0.0.1:$((6000 + i))" get demo:marker --local)
      echo "node$i local read demo:marker = $v"
      [[ $v == after-failover ]] || { echo "node$i diverged" >&2; exit 1; }
    done
    echo -e "\nall five nodes converged to the same state"
    exit 0
  fi
  sleep 0.2
done
echo "nodes did not converge" >&2
exit 1
