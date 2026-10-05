#!/bin/bash
# Usage: run.sh <standard|extreme> <runs> <seconds>   (image from $IMAGE)
# Starts a fresh sqld per run and fails if any run wedges, crashes, logs a
# stale write-lock slot, or fails its consistency checks. Extra `docker run`
# arguments (e.g. "-e SQLD_CHECKPOINT_INTERVAL_S=60") come from $SQLD_ARGS.
set -u
MODE=$1; RUNS=$2; DUR=$3; DIR=$(cd "$(dirname "$0")" && pwd)
SCRIPT=$DIR/stress.py; [ "$MODE" = extreme ] && SCRIPT=$DIR/extreme.py
fails=0
for i in $(seq 1 "$RUNS"); do
  docker rm -f -v sqld >/dev/null 2>&1 || true
  docker run -d --name sqld -p 18080:8080 -e SQLD_NODE=primary -e SQLD_DB_PATH=/var/lib/sqld/data.sqld \
    -e SQLD_HTTP_LISTEN_ADDR=0.0.0.0:8080 ${SQLD_ARGS:-} "$IMAGE" >/dev/null
  until curl -sf -X POST -H content-type:application/json --data '{"statements":["SELECT 1"]}' http://127.0.0.1:18080/ >/dev/null; do sleep 0.5; done
  rc=0; DURATION=$DUR python3 "$SCRIPT" > "run-$MODE-$i.log" 2>&1 || rc=$?
  running=$(docker inspect -f '{{.State.Running}}' sqld)
  stale=$(docker logs sqld 2>&1 | grep -c 'has not progressed' || true)
  panics=$(docker logs sqld 2>&1 | grep -c 'panicked' || true)
  echo "$MODE run $i/$RUNS: rc=$rc running=$running stale_slots=$stale panics=$panics"
  grep -E 'CONSISTENCY|^CHECKS|PROBLEM|WEDGED' "run-$MODE-$i.log" || true
  if [ "$rc" != 0 ] || [ "$running" != true ] || [ "$stale" != 0 ] || [ "$panics" != 0 ]; then fails=$((fails+1)); fi
done
docker rm -f -v sqld >/dev/null 2>&1 || true
[ "$fails" = 0 ]
