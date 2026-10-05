#!/bin/bash
# Usage: overload.sh <seconds>   (image from $IMAGE, extra `docker run` args from $SQLD_ARGS)
#
# Overload: the extreme client mix against a sqld capped at 1 CPU and 1 GB, so
# writes arrive faster than they can be served. Measures how far the backlog
# grows (sqld threads) and how long sqld takes to serve a write again once the
# load stops, then runs extreme.py's consistency checks. Fails if sqld exits,
# panics, logs a stale slot, fails the checks, or takes more than
# $MAX_RECOVERY_S (default 60) to recover.
set -u
DUR=$1; DIR=$(cd "$(dirname "$0")" && pwd); MAX_RECOVERY_S=${MAX_RECOVERY_S:-60}
URL=http://127.0.0.1:18080
docker rm -f -v sqld >/dev/null 2>&1 || true
docker run -d --name sqld --cpus=1 --memory=1g --ulimit nofile=65536:524288 -p 18080:8080 \
  -e SQLD_NODE=primary -e SQLD_DB_PATH=/var/lib/sqld/data.sqld -e SQLD_HTTP_LISTEN_ADDR=0.0.0.0:8080 \
  ${SQLD_ARGS:-} "$IMAGE" >/dev/null
until curl -sf -X POST -H content-type:application/json --data '{"statements":["SELECT 1"]}' $URL/ >/dev/null; do sleep 0.5; done

threads() {
  local n=0 p
  for p in $(docker top sqld -o pid 2>/dev/null | tail -n +2); do
    n=$((n + $(sudo ls /proc/$p/task 2>/dev/null | wc -l)))
  done
  echo $n
}
LOCK='{"requests":[{"type":"execute","stmt":{"sql":"BEGIN IMMEDIATE"}},{"type":"execute","stmt":{"sql":"ROLLBACK"}},{"type":"close"}]}'
write_ok() {  # the write lock can be taken within 1 s
  curl -s -m 1 -X POST -H content-type:application/json --data "$LOCK" $URL/v2/pipeline 2>/dev/null \
    | python3 -c 'import json,sys; r=json.load(sys.stdin)["results"]; sys.exit(0 if all(x["type"]=="ok" for x in r) else 1)' 2>/dev/null
}

N_HOT=60 N_ABANDON=10 N_HOG=3 N_READ=8 WEDGE_SECS=100000 DURATION=$DUR \
  python3 "$DIR/extreme.py" > run-overload.log 2>&1 &
XP=$!
peak=0; end=$((SECONDS + DUR))
while [ $SECONDS -lt $end ]; do
  t=$(threads); [ "$t" -gt "$peak" ] && peak=$t
  echo "t=$((DUR - end + SECONDS))s threads=$t" >> run-overload-threads.log
  sleep 5
done
# extreme.py stops its workers at DURATION; time until a write gets through.
stop=$SECONDS; recovered=-1
while [ $((SECONDS - stop)) -lt 300 ]; do
  if write_ok; then recovered=$((SECONDS - stop)); break; fi
  sleep 1
done
t_after=$(threads)
wait $XP; rc=$?
running=$(docker inspect -f '{{.State.Running}}' sqld)
stale=$(docker logs sqld 2>&1 | grep -c 'has not progressed' || true)
panics=$(docker logs sqld 2>&1 | grep -c 'panicked' || true)
gave_up=$(docker logs sqld 2>&1 | grep 'write lock not acquired' | tail -1 | grep -oE '[0-9]+ waiters gave up' || echo "0 waiters gave up")
echo "overload: rc=$rc running=$running stale_slots=$stale panics=$panics peak_threads=$peak threads_after_recovery=$t_after recovered_after_s=$recovered ($gave_up)"
grep -E '^t=|CONSISTENCY|^CHECKS|PROBLEM|WEDGED' run-overload.log | tail -4 || true
docker logs sqld > run-overload-sqld.log 2>&1
docker rm -f -v sqld >/dev/null 2>&1 || true
[ "$rc" = 0 ] && [ "$running" = true ] && [ "$stale" = 0 ] && [ "$panics" = 0 ] \
  && [ "$recovered" -ge 0 ] && [ "$recovered" -le "$MAX_RECOVERY_S" ]
