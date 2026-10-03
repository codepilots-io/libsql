"""Extreme stress for sqld's write-lock manager.

Workers (all against one sqld):
  hot        - single-statement upserts on one hot row, one stream per request
               (closed immediately). Counted exactly.
  churn      - bulk inserts/deletes that grow the WAL -> frequent checkpoints.
  abandon    - BEGIN IMMEDIATE + 2 inserts + COMMIT with a 5-200 ms client
               timeout: the client hangs up mid-queue / mid-transaction.
  hog        - interactive transaction over a baton stream: BEGIN IMMEDIATE +
               2 inserts, hold 0-8 s (sqld steals after 5 s), then COMMIT or
               vanish without closing.
  read       - read-only queries that pin WAL frames.

Failure modes reported:
  WEDGED   - no successful hot write for WEDGE_SECS
  MISMATCH - hot counter or atomicity / commit-outcome check failed
Exit codes: 0 pass, 2 wedged, 3 mismatch.
"""
import json, os, random, sys, threading, time, urllib.request, collections, uuid
import http.client
from urllib.parse import urlparse

URL = os.environ.get("SQLD_URL", "http://127.0.0.1:18080")
N_HOT = int(os.environ.get("N_HOT", "150"))
N_CHURN = int(os.environ.get("N_CHURN", "4"))
N_ABANDON = int(os.environ.get("N_ABANDON", "30"))
N_HOG = int(os.environ.get("N_HOG", "5"))
N_READ = int(os.environ.get("N_READ", "20"))
DURATION = int(os.environ.get("DURATION", "600"))
WEDGE_SECS = int(os.environ.get("WEDGE_SECS", "45"))

stats = collections.Counter()
lock = threading.Lock()
last_hot_ok = time.time()
stop = threading.Event()
hog_outcomes = {}  # token -> "ok" | "err" | "unknown"


def post(body, timeout):
    r = urllib.request.Request(URL + "/v2/pipeline", json.dumps(body).encode(),
                               {"content-type": "application/json"})
    with urllib.request.urlopen(r, timeout=timeout) as resp:
        return json.load(resp)


_local = threading.local()


def post_ka(body, timeout):
    """POST over a per-thread persistent HTTP connection (like the app's client)."""
    u = urlparse(URL)
    conn = getattr(_local, "conn", None)
    if conn is None:
        conn = _local.conn = http.client.HTTPConnection(u.hostname, u.port, timeout=timeout)
    try:
        conn.request("POST", "/v2/pipeline", json.dumps(body), {"content-type": "application/json"})
        resp = conn.getresponse()
        data = resp.read()
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status}")
        return json.loads(data)
    except Exception:
        conn.close()
        _local.conn = None
        raise


def stmt(sql):
    return {"type": "execute", "stmt": {"sql": sql}}


def txn_batch(stmts):
    """Like @libsql/client's batch(): BEGIN IMMEDIATE, each step only if the
    previous one succeeded, COMMIT, and ROLLBACK if anything failed."""
    steps = [{"stmt": {"sql": "BEGIN IMMEDIATE"}}]
    for sql in stmts:
        steps.append({"condition": {"type": "ok", "step": len(steps) - 1}, "stmt": {"sql": sql}})
    steps.append({"condition": {"type": "ok", "step": len(steps) - 1}, "stmt": {"sql": "COMMIT"}})
    steps.append({"condition": {"type": "not", "cond": {"type": "ok", "step": len(steps) - 1}},
                  "stmt": {"sql": "ROLLBACK"}})
    return {"type": "batch", "batch": {"steps": steps}}


def batch_committed(out):
    res = out["results"][0]
    if res.get("type") != "ok":
        return False
    r = res["response"]["result"]
    commit_idx = len(r["step_results"]) - 2
    return r["step_results"][commit_idx] is not None and r["step_errors"][commit_idx] is None


def errors(out):
    return [x["error"]["message"] for x in out["results"] if x.get("type") == "error"]


def bump(key, n=1):
    with lock:
        stats[key] += n


def hot():
    global last_hot_ok
    while not stop.is_set():
        try:
            out = post_ka({"requests": [stmt("INSERT INTO counter(k, n) VALUES ('h', 1) "
                                          "ON CONFLICT(k) DO UPDATE SET n = n + 1"),
                                     {"type": "close"}]}, 60)
            if errors(out):
                bump("hot_sqlite_err")
            else:
                bump("hot_ok")
                with lock:
                    last_hot_ok = time.time()
        except Exception as e:
            bump("hot_uncertain")
            bump("exc:" + type(e).__name__ + ":" + str(e)[:40])


def churn():
    while not stop.is_set():
        try:
            post_ka({"requests": [
                stmt("INSERT INTO pad(b) SELECT randomblob(3000) FROM (WITH RECURSIVE c(x) AS "
                     "(SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<2000) SELECT x FROM c)"),
                stmt("DELETE FROM pad WHERE id < (SELECT max(id) - 15000 FROM pad)"),
                {"type": "close"}]}, 60)
            bump("churn")
        except Exception:
            bump("churn_exc")


def abandon():
    while not stop.is_set():
        t = uuid.uuid4().hex
        try:
            post({"requests": [txn_batch([f"INSERT INTO pair(token, part) VALUES ('{t}', 1)",
                                          f"INSERT INTO pair(token, part) VALUES ('{t}', 2)"]),
                               {"type": "close"}]},
                 random.uniform(0.005, 0.2))
            bump("abandon_completed")
            time.sleep(random.uniform(0.05, 0.15))
        except Exception:
            bump("abandon_hungup")
        time.sleep(random.uniform(0.05, 0.15))


def hog():
    while not stop.is_set():
        t = uuid.uuid4().hex
        try:
            out = post({"baton": None, "requests": [stmt("BEGIN IMMEDIATE")]}, 30)
            baton = out.get("baton")
            if errors(out) or not baton:
                bump("hog_begin_fail")
                if baton:
                    post({"baton": baton, "requests": [{"type": "close"}]}, 30)
                continue
            out = post({"baton": baton, "requests": [
                stmt(f"INSERT INTO pair(token, part) VALUES ('{t}', 1)"),
                stmt(f"INSERT INTO pair(token, part) VALUES ('{t}', 2)")]}, 30)
            baton = out.get("baton")
            if errors(out) or not baton:
                # Lock stolen / txn timed out between steps; sqld rolled back.
                bump("hog_insert_fail")
                with lock:
                    hog_outcomes[t] = "unknown"
                if baton:
                    post({"baton": baton, "requests": [stmt("ROLLBACK"), {"type": "close"}]}, 30)
                continue
            time.sleep(random.uniform(0, 8))
            if random.random() < 0.3:
                bump("hog_vanished")  # never commit, never close: stream expiry
                with lock:
                    hog_outcomes[t] = "unknown"
                continue
            try:
                out = post({"baton": baton, "requests": [stmt("COMMIT"), {"type": "close"}]}, 30)
                res = "err" if errors(out) else "ok"
            except urllib.error.HTTPError:
                res = "err"  # stream already expired / txn stolen
            except Exception:
                res = "unknown"
            with lock:
                hog_outcomes[t] = res
                stats["hog_" + res] += 1
        except Exception:
            bump("hog_exc")


def read():
    while not stop.is_set():
        try:
            post_ka({"requests": [stmt("SELECT count(*), max(id) FROM pad"),
                               stmt("SELECT count(*) FROM pair"), {"type": "close"}]}, 60)
            bump("read")
        except Exception:
            bump("read_exc")


def query(sql):
    out = post({"requests": [stmt(sql), {"type": "close"}]}, 120)
    return out["results"][0]["response"]["result"]["rows"]


def main():
    post({"requests": [
        stmt("CREATE TABLE IF NOT EXISTS counter(k TEXT PRIMARY KEY, n INTEGER)"),
        stmt("CREATE TABLE IF NOT EXISTS pad(id INTEGER PRIMARY KEY, b BLOB)"),
        stmt("CREATE TABLE IF NOT EXISTS pair(token TEXT, part INTEGER)"),
        stmt("CREATE INDEX IF NOT EXISTS pair_token ON pair(token)"),
        {"type": "close"}]}, 30)
    workers = ([hot] * N_HOT + [churn] * N_CHURN + [abandon] * N_ABANDON
               + [hog] * N_HOG + [read] * N_READ)
    for w in workers:
        threading.Thread(target=w, daemon=True).start()
    start = time.time()
    wedged = False
    while time.time() - start < DURATION:
        time.sleep(15)
        with lock:
            snap = dict(stats)
            stalled = time.time() - last_hot_ok
        print(f"t={int(time.time()-start):4d}s stalled={stalled:.0f}s {snap}", flush=True)
        if stalled > WEDGE_SECS:
            print(f"WEDGED: no successful hot write for {stalled:.0f}s", flush=True)
            wedged = True
            break
    stop.set()
    if wedged:
        sys.exit(2)
    time.sleep(70)  # let in-flight requests finish and abandoned streams expire

    problems = []
    n = query("SELECT n FROM counter WHERE k='h'")
    views = int(n[0][0]["value"]) if n else 0
    ok, unc = stats["hot_ok"], stats["hot_uncertain"]
    if not ok <= views <= ok + unc:
        problems.append(f"hot counter {views} outside [{ok}, {ok + unc}]")

    parts = {r[0]["value"]: int(r[1]["value"])
             for r in query("SELECT token, count(*) FROM pair GROUP BY token")}
    torn = [t for t, c in parts.items() if c != 2]
    if torn:
        problems.append(f"{len(torn)} torn transactions (rows != 2), e.g. {torn[:3]}")
    for t, res in hog_outcomes.items():
        if res == "ok" and parts.get(t) != 2:
            problems.append(f"hog {t} acknowledged commit but rows={parts.get(t)}")
        if res == "err" and t in parts:
            problems.append(f"hog {t} commit rejected but rows={parts[t]}")
    acked = sum(1 for r in hog_outcomes.values() if r == "ok")
    print(f"CHECKS hot={views}/{ok}+{unc} pairs={len(parts)} torn={len(torn)} "
          f"hog_acked={acked} hog_outcomes={collections.Counter(hog_outcomes.values())} "
          f"-> {'OK' if not problems else 'MISMATCH'}", flush=True)
    for p in problems[:10]:
        print("  PROBLEM:", p, flush=True)
    sys.exit(0 if not problems else 3)


if __name__ == "__main__":
    main()
