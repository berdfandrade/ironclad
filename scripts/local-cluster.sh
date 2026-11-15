#!/usr/bin/env bash
# Runs an N-node Ironclad cluster on localhost until Ctrl-C.
#
#   scripts/local-cluster.sh [nodes=3] [data_dir=./data]
#
# Node i listens for clients on 127.0.0.1:600i and for peers on 127.0.0.1:700i.
# Extra server flags can be passed via IRONCLAD_FLAGS.
set -euo pipefail

N=${1:-3}
DATA=${2:-./data}
BIN=${IRONCLAD_BIN:-./target/release/ironclad-server}
[[ -x "$BIN" ]] || cargo build --release --bin ironclad-server

members=()
for i in $(seq 1 "$N"); do
  members+=(--member "$i=127.0.0.1:$((6000 + i))=127.0.0.1:$((7000 + i))")
done

pids=()
trap 'kill "${pids[@]}" 2>/dev/null; wait' EXIT INT TERM
for i in $(seq 1 "$N"); do
  mkdir -p "$DATA/$i"
  # shellcheck disable=SC2086
  "$BIN" --id "$i" --data-dir "$DATA/$i" "${members[@]}" ${IRONCLAD_FLAGS:-} 2>>"$DATA/$i/server.log" &
  pids+=($!)
done

addrs=$(seq -s, -f '127.0.0.1:%g' 6001 $((6000 + N)))
echo "cluster of $N nodes running; logs in $DATA/<id>/server.log"
echo "try: ./target/release/ironclad-cli -a $addrs status"
wait
