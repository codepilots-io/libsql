"""Stress a local sqld to reproduce the write-lock wedge.

Workload: many short autocommit upserts on one hot row (a page-view counter), each its
own HTTP pipeline that closes the stream immediately (like @libsql/client's
single execute), plus a few slow bulk writes that grow the WAL and force
frequent checkpoints. A watchdog probes BEGIN IMMEDIATE and declares a wedge
when no write has succeeded for WEDGE_SECS.
"""
import json, os, sys, threading, time, urllib.request, collections

URL = os.environ.get("SQLD_URL", "http://127.0.0.1:18080")
HOT_WRITERS = int(os.environ.get("HOT_WRITERS", "40"))
SLOW_WRITERS = int(os.environ.get("SLOW_WRITERS", "2"))
SLOW_ROWS = int(os.environ.get("SLOW_ROWS", "3000"))
DURATION = int(os.environ.get("DURATION", "600"))
WEDGE_SECS = int(os.environ.get("WEDGE_SECS", "45"))
REQ_TIMEOUT = 60

stats = collections.Counter()
last_ok = time.time()
lock = threading.Lock()
stop = threading.Event()


def pipeline(stmts, timeout=REQ_TIMEOUT):
    reqs = [{"type": "execute", "stmt": {"sql": s}} for s in stmts] + [{"type": "close"}]
    body = json.dumps({"requests": reqs}).encode()
    r = urllib.request.Request(URL + "/v2/pipeline", body, {"content-type": "application/json"})
    with urllib.request.urlopen(r, timeout=timeout) as resp:
        out = json.load(resp)
    errs = [x["error"]["message"] for x in out["results"] if x.get("type") == "error"]
    return errs


def record(kind, errs, t0):
    global last_ok
    with lock:
        if errs:
            stats[kind + "_err"] += 1
            stats["err:" + errs[0][:60]] += 1
        else:
            stats[kind + "_ok"] += 1
            last_ok = time.time()
        stats[kind + "_maxms"] = max(stats[kind + "_maxms"], int((time.time() - t0) * 1000))


def hot_writer():
    while not stop.is_set():
        t0 = time.time()
        try:
            errs = pipeline([
                "INSERT INTO counter(day, views) VALUES ('d', 1) "
                "ON CONFLICT(day) DO UPDATE SET views = views + 1"
            ])
        except Exception as e:  # timeout / connection error
            errs = [type(e).__name__]
        record("hot", errs, t0)


def slow_writer():
    while not stop.is_set():
        t0 = time.time()
        try:
            errs = pipeline([
                f"INSERT INTO pad(b) SELECT randomblob(3000) FROM "
                f"(WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<{SLOW_ROWS}) SELECT x FROM c)",
                "DELETE FROM pad WHERE id < (SELECT max(id) - 20000 FROM pad)",
            ])
        except Exception as e:
            errs = [type(e).__name__]
        record("slow", errs, t0)


def probe():
    t0 = time.time()
    try:
        errs = pipeline(["BEGIN IMMEDIATE", "ROLLBACK"], timeout=10)
        return f"lock={'ok' if not errs else errs[0][:40]} {int((time.time()-t0)*1000)}ms"
    except Exception as e:
        return f"lock=HANG({type(e).__name__}) {int((time.time()-t0)*1000)}ms"


def main():
    pipeline([
        "CREATE TABLE IF NOT EXISTS counter(day TEXT PRIMARY KEY, views INTEGER)",
        "CREATE TABLE IF NOT EXISTS pad(id INTEGER PRIMARY KEY, b BLOB)",
    ])
    threads = [threading.Thread(target=hot_writer, daemon=True) for _ in range(HOT_WRITERS)]
    threads += [threading.Thread(target=slow_writer, daemon=True) for _ in range(SLOW_WRITERS)]
    for t in threads:
        t.start()
    start = time.time()
    prev = collections.Counter()
    while time.time() - start < DURATION:
        time.sleep(5)
        with lock:
            snap = stats.copy()
            stalled = time.time() - last_ok
        delta = {k: snap[k] - prev[k] for k in ("hot_ok", "hot_err", "slow_ok", "slow_err")}
        prev = snap
        errs = {k[4:]: v for k, v in snap.items() if k.startswith("err:")}
        print(f"t={int(time.time()-start):4d}s {delta} stalled={stalled:.0f}s {probe()} errs={errs}", flush=True)
        if stalled > WEDGE_SECS:
            print(f"WEDGED: no successful write for {stalled:.0f}s", flush=True)
            stop.set()
            sys.exit(2)
    stop.set()
    time.sleep(REQ_TIMEOUT if stats["hot_maxms"] > 30000 else 3)  # let in-flight writes settle
    out = json.load(urllib.request.urlopen(urllib.request.Request(
        URL + "/v2/pipeline",
        json.dumps({"requests": [{"type": "execute", "stmt": {"sql": "SELECT views FROM counter WHERE day='d'"}}, {"type": "close"}]}).encode(),
        {"content-type": "application/json"}), timeout=30))
    views = int(out["results"][0]["response"]["result"]["rows"][0][0]["value"])
    ok = stats["hot_ok"]
    # A request that timed out client-side may still have committed, so views can
    # exceed ok by at most the number of client-side timeouts; never fall below.
    # Client-side exceptions (timeouts, resets) leave the outcome unknown; a
    # SQLite error response means the server reported the write as failed.
    uncertain = sum(v for k, v in stats.items() if k.startswith("err:") and not k.startswith("err:SQLite"))
    consistent = ok <= views <= ok + uncertain
    print(f"CONSISTENCY views={views} client_ok={ok} uncertain={uncertain} sqlite_errs={stats['hot_err'] - uncertain if stats['hot_err'] >= uncertain else '?'} -> {'OK' if consistent else 'MISMATCH'}", flush=True)
    print("NO WEDGE within duration", flush=True)
    sys.exit(0 if consistent else 3)


if __name__ == "__main__":
    main()
