# sqld write-lock stress tests

Two harnesses that hammer a running sqld over its HTTP API and check that it
neither stalls nor loses or tears writes.

- `stress.py` — the reproduction: 40 clients upserting one hot row, each
  request on its own stream that closes immediately, plus bulk writers that
  grow the WAL and force frequent checkpoints. Fails if no write succeeds for
  45 s, or if the counter does not match the acknowledged writes.
- `extreme.py` — 150 hot-row writers, 30 clients that start a transaction and
  hang up after 5–200 ms, 5 clients that hold a transaction up to 8 s (forcing
  sqld's 5 s lock stealing) and then commit or vanish, WAL churn and readers.
  Checks the counter, that every two-row transaction is all-or-nothing, and
  that acknowledged commits exist and rejected ones do not.

- `overload.sh` — the extreme client mix against a sqld limited to 1 CPU and
  1 GB, so writes arrive faster than they are served. Reports peak threads and
  how long sqld takes to accept a write once the load stops; fails on a crash,
  failed checks, or no recovery within 60 s.

Run against any image (fresh container per run; fails on stall, crash, stale
write-lock slot or failed checks):

```sh
IMAGE=ghcr.io/tursodatabase/libsql-server:latest codepilots/stress/run.sh standard 3 180
IMAGE=ghcr.io/codepilots-io/libsql-server:latest codepilots/stress/run.sh extreme 2 600
```

Or use the "Stress-test libsql-server image" workflow. The harnesses need
Python 3 only (standard library).
