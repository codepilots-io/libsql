# Changelog — codepilots-io/libsql

This fork tracks [tursodatabase/libsql](https://github.com/tursodatabase/libsql)
and carries fixes to **libsql-server (sqld)** that are not upstream. Releases
use the upstream sqld version they are based on; the image name
(`ghcr.io/codepilots-io/libsql-server`) tells them apart from upstream's
builds. A further fix on the same upstream base gets the next patch version.

## 0.24.33 — 2026-10-04

Based on upstream `f8fb14f3` (sqld 0.24.33, the source of
`ghcr.io/tursodatabase/libsql-server:latest` as of August 2026). All code
changes are in `libsql-server/src/connection/connection_manager.rs`, the
write-lock scheduler that serialises writers across connections.

### Fixed

- **Write lock handed to a connection that is no longer waiting** (`8d47b6606`).
  A writer (often a WAL auto-checkpoint) that joins the write queue and then
  gives up with `SQLITE_BUSY` — because a checkpoint holds the lock, or the
  queue sync token changed — left its queue entry behind. When the entry came
  up, the lock was handed to that connection, frequently one that had already
  closed. Nothing ever released it: every writer waited forever, busy-looping
  on an expired deadline, so the server pinned all CPUs and stopped accepting
  writes while reads still worked. The lock-stealing timeout does not apply to
  that slot state, so it never recovered on its own. Precursor in the logs:
  `begin_write_txn ... failed to rollback: cannot rollback - no transaction is active`.
  Workloads with many small autocommit writes over short-lived HTTP streams
  (e.g. counters written per request) hit it within seconds under load.
- **Server abort when a connection closes at the wrong moment** (`d1682fe72`).
  `close()` checked that it owned the lock slot, dropped the guard, then
  released it. A slot in the `Failure` state can be handed to the next waiter
  in between; `release()` then failed `assert_eq!(slot.id, self.id)` inside a
  SQLite callback and the whole process aborted (exit 133).
- **Deadlock in write-lock stealing** (`77d038f28`). After a transaction holds
  the lock for more than 5 s, a waiter force-rolls it back. The stealer blocked
  on the owner's connection mutex; if the owner had meanwhile finished and was
  parked waiting for the lock again (holding that mutex), and the lock was then
  handed to the stealer, both waited on each other forever. The stealer now only
  `try_lock`s the owner, re-checks that it still holds the same slot, and backs
  off briefly otherwise.

### Changed

- Waiters never busy-spin: a waiter on a stalled slot sleeps at least 10 ms.
- A stalled slot is logged once per waiter at `WARN`:
  `conn N waiting on slot ... that has not progressed for ... (owner connection still open: ..., queue len: ...)`.
  It should never appear; if it does, it says which kind of stall it is.

### Build

- `Dockerfile.codepilots` builds on Debian bookworm with the upstream image
  layout (entrypoint, `gosu`, `sqld` user, volume, ports). Upstream's bullseye
  images no longer build because bullseye's security archive returns 404.
- Image: `ghcr.io/codepilots-io/libsql-server:0.24.33` (linux/amd64).

### Verification

Reproduction and harnesses are in [`codepilots/stress`](codepilots/stress).

- Upstream 0.24.33 wedges in every run of the standard reproduction (40
  writers on one hot row, WAL churn), usually within 30–90 s.
- This version: 10/10 standard runs and 15/15 extreme runs (150 writers,
  clients hanging up mid-transaction, transactions held past the 5 s steal
  timeout, at 4, 2 and 1 CPUs and under CPU contention) with no stall, no
  panic, no torn transaction, and every acknowledged commit present.
- A 60-minute soak (threads, file descriptors and memory flat), 4× `kill -9`
  under load (all acknowledged commits survive) and 3× disk-full under load
  (writes rejected cleanly, then resume).
- Upstream unit tests: the `connection::` suite passes; `test_many_concurrent`
  is flaky at the same rate as on upstream (2/20 on both).
