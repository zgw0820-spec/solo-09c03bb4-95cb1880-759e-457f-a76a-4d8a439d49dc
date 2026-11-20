"""End-to-end verification of per-connection pause/resume (历史补报暂缓).

Scenarios (all against one gateway instance):

  1. Two simulated recorders run at the same time (slots r1/r2), each on
     its own single TCP connection.
  2. Pausing connection A freezes its ack stream (gateway stops reading
     that TCP connection) while connection B keeps flowing.
  3. A successful upgrade v1 -> v2 during the pause keeps the pause in
     effect: no new acks for A across the handover, B unaffected.
  4. Resuming A completes the stream: ids 1..N acked exactly once, in
     send order, split across v1/v2.
  5. A failed new-version startup (rollback) during a pause also keeps
     the pause in effect; the old collector keeps serving the rest.
  6. Half-line preservation: a partial JSON line already read into the
     collector buffer survives pause + upgrade; after resume the line is
     completed on the same connection and acked exactly once (clean
     version boundary v2: 1..5, v4: 6..12).
  7. Pause requested while a handover is in flight is rejected with a
     retry hint (409 + retry=true); retrying after the handover works.
  8. Error paths: unknown peer, double pause, resume-when-not-paused.

Run:  python tests/pause_test.py
"""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)


def http(method: str, url: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return json.loads(exc.read().decode())


def wait_status(base: str, timeout: float = 15.0) -> dict:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            return http("GET", base + "/api/status")
        except (urllib.error.URLError, ConnectionResetError) as exc:
            last = exc
            time.sleep(0.25)
    raise RuntimeError(f"gateway never came up: {last}")


def wait_event(base: str, event_type: str, timeout: float = 15.0,
               **match) -> dict:
    deadline = time.monotonic() + timeout
    cursor = 0
    while time.monotonic() < deadline:
        ev = http("GET", f"{base}/api/events?cursor={cursor}")
        cursor = ev["cursor"]
        for e in ev["events"]:
            if e.get("type") != event_type:
                continue
            if all(e.get(k) == v for k, v in match.items()):
                return e
        time.sleep(0.2)
    raise AssertionError(f"event {event_type} {match} not seen within {timeout}s")


def wait_conn(base: str, peer: str, timeout: float = 10.0, **attrs) -> dict:
    """Poll /api/status until the connection row matches all attrs."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        st = http("GET", base + "/api/status")
        for c in st["connections"]:
            if c["peer"] == peer and all(c.get(k) == v for k, v in attrs.items()):
                return c
        time.sleep(0.2)
    raise AssertionError(f"connection {peer} never reached state {attrs}")


def open_conns(base: str) -> list[dict]:
    st = http("GET", base + "/api/status")
    return [c for c in st["connections"] if not c["closed"]]


def peer_acks(base: str, peer: str) -> tuple[int, dict]:
    v = http("GET", base + "/api/verify")
    for c in v["connections"]:
        if c["peer"] == peer:
            return c["count"], c
    return 0, {}


def wait_peer_acks(base: str, peer: str, expected: int,
                   timeout: float = 40.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        n, conn = peer_acks(base, peer)
        if n >= expected:
            return conn
        time.sleep(0.3)
    raise AssertionError(f"timed out waiting for {expected} acks on {peer}")


class Check:
    def __init__(self) -> None:
        self.passed = 0
        self.failed: list[str] = []

    def expect(self, cond: bool, name: str, detail: str = "") -> None:
        if cond:
            self.passed += 1
            print(f"  PASS  {name}")
        else:
            self.failed.append(f"{name} {detail}")
            print(f"  FAIL  {name} {detail}")


def start_pair(base: str, count: int, interval: float) -> None:
    r1 = http("POST", base + "/api/recorder/start",
              {"count": count, "interval": interval, "gap_at": 0,
               "gap_seconds": 0, "recorder_id": "r1"})
    assert r1.get("ok"), f"start r1: {r1}"
    r2 = http("POST", base + "/api/recorder/start",
              {"count": count, "interval": interval, "gap_at": 0,
               "gap_seconds": 0, "recorder_id": "r2"})
    assert r2.get("ok"), f"start r2: {r2}"


def wait_recorders_idle(base: str, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        st = http("GET", base + "/api/status")
        if not any(r["alive"] for r in st["recorders"]):
            return
        time.sleep(0.3)
    raise AssertionError("recorders did not exit in time")


def peers_by_sensor(base: str, timeout: float = 15.0) -> dict[str, str]:
    """Map sensor name -> peer for the currently open connections."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        conns = open_conns(base)
        m = {c["sensor"]: c["peer"] for c in conns if c.get("sensor")}
        if "freezer-1" in m and "freezer-2" in m:
            return m
        time.sleep(0.25)
    raise AssertionError("two live recorder connections never appeared")


def main() -> int:
    run_dir = tempfile.mkdtemp(prefix="gw-pause-")
    port_tcp = 19011
    port_web = 18011
    env = dict(os.environ,
               GATEWAY_RUN_DIR=run_dir,
               GATEWAY_TCP_PORT=str(port_tcp),
               GATEWAY_WEB_PORT=str(port_web),
               COLLECTOR_FAIL_PATH=os.path.join(run_dir, "fail"),
               COLLECTOR_INITIAL_VERSION="v1")
    proc = subprocess.Popen(
        [sys.executable, "-m", "app.main"], cwd=ROOT, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    base = f"http://127.0.0.1:{port_web}"
    check = Check()
    try:
        wait_status(base)
        http("POST", base + "/api/events/reset")

        print("== scenario 1: two recorders at the same time ==")
        count1 = 40
        start_pair(base, count1, 0.1)
        peers = peers_by_sensor(base)
        peer_a, peer_b = peers["freezer-1"], peers["freezer-2"]
        st = http("GET", base + "/api/status")
        check.expect(sum(1 for r in st["recorders"] if r["alive"]) == 2,
                     "two simulated recorders running simultaneously",
                     str(st["recorders"]))
        check.expect(len(open_conns(base)) == 2,
                     "two live TCP connections tracked")
        time.sleep(1.5)  # let both streams ack a prefix

        print("== scenario 2: pause A, B keeps flowing ==")
        r = http("POST", base + "/api/connections/pause", {"peer": peer_a})
        check.expect(r.get("ok") is True, "pause request accepted", str(r))
        ca = wait_conn(base, peer_a, paused=True, pause_pending=False)
        check.expect(True, "page state shows pause actually in effect",
                     str(ca))
        n_a, _ = peer_acks(base, peer_a)
        n_b, _ = peer_acks(base, peer_b)
        check.expect(0 < n_a < count1, "A acked a prefix before pausing",
                     f"n_a={n_a}")
        time.sleep(2.0)  # both recorders keep sending; A must not advance
        n_a2, _ = peer_acks(base, peer_a)
        n_b2, _ = peer_acks(base, peer_b)
        check.expect(n_a2 == n_a, "paused connection produced no new acks",
                     f"{n_a} -> {n_a2}")
        check.expect(n_b2 > n_b, "other connection kept acking while A paused",
                     f"{n_b} -> {n_b2}")

        print("== scenario 3: successful upgrade keeps the pause ==")
        up = http("POST", base + "/api/upgrade",
                  {"new_version": "v2", "simulate_fail": False})
        check.expect(up.get("ok") is True, "upgrade v1->v2 accepted", str(up))
        done = wait_event(base, "upgrade_completed")
        check.expect(done["connections_forwarded"] == 2,
                     "both connections forwarded on the same TCP fds",
                     str(done))
        st = http("GET", base + "/api/status")
        check.expect(st["current_version"] == "v2", "v2 now active")
        ca = wait_conn(base, peer_a, paused=True)
        n_a3, _ = peer_acks(base, peer_a)
        n_b3, _ = peer_acks(base, peer_b)
        check.expect(n_a3 == n_a, "pause survived the upgrade (A still frozen)",
                     f"{n_a} -> {n_a3}")
        check.expect(n_b3 >= n_b2, "B still flowing after upgrade",
                     f"{n_b2} -> {n_b3}")

        print("== scenario 4: resume A, stream completes exactly once ==")
        r = http("POST", base + "/api/connections/resume", {"peer": peer_a})
        check.expect(r.get("ok") is True, "resume request accepted", str(r))
        wait_conn(base, peer_a, paused=False, pause_pending=False)
        conn_a = wait_peer_acks(base, peer_a, count1)
        conn_b = wait_peer_acks(base, peer_b, count1)
        check.expect(conn_a["contiguous_from_1"] and conn_a["unique"],
                     "A: ids 1..N contiguous, no gaps after resume",
                     str(conn_a["ids"]))
        check.expect(conn_b["contiguous_from_1"] and conn_b["unique"],
                     "B: ids 1..N contiguous", str(conn_b["ids"]))
        check.expect(set(conn_a["versions"]) == {"v1", "v2"},
                     "A's stream split across v1 (pre-pause) and v2 (post-resume)",
                     str(conn_a["versions"]))
        v = http("GET", base + "/api/verify")
        check.expect(not v["duplicate_acks"], "no duplicate acks so far")

        print("== scenario 5: failed startup (rollback) keeps the pause ==")
        count2 = 30
        wait_recorders_idle(base)  # round-1 recorders free their slots
        start_pair(base, count2, 0.1)
        peers2 = peers_by_sensor(base)
        pa2, pb2 = peers2["freezer-1"], peers2["freezer-2"]
        check.expect(pa2 != peer_a and pb2 != peer_b,
                     "second round uses fresh connections")
        time.sleep(1.0)
        r = http("POST", base + "/api/connections/pause", {"peer": pa2})
        check.expect(r.get("ok") is True, "pause accepted before failed upgrade")
        wait_conn(base, pa2, paused=True, pause_pending=False)
        na, _ = peer_acks(base, pa2)
        nb, _ = peer_acks(base, pb2)
        failed = http("POST", base + "/api/upgrade",
                      {"new_version": "v3", "simulate_fail": True})
        check.expect(failed.get("ok") is False and failed.get("rolled_back"),
                     "failed start reported as rolled back", str(failed))
        wait_event(base, "upgrade_failed", new_version="v3")
        st = http("GET", base + "/api/status")
        check.expect(st["current_version"] == "v2",
                     "old version still active after rollback")
        time.sleep(1.0)
        na2, _ = peer_acks(base, pa2)
        nb2, _ = peer_acks(base, pb2)
        check.expect(na2 == na, "pause survived the failed startup",
                     f"{na} -> {na2}")
        check.expect(nb2 > nb, "other connection kept flowing through rollback",
                     f"{nb} -> {nb2}")
        r = http("POST", base + "/api/connections/resume", {"peer": pa2})
        check.expect(r.get("ok") is True, "resume after rollback accepted")
        conn_a2 = wait_peer_acks(base, pa2, count2)
        wait_peer_acks(base, pb2, count2)
        check.expect(conn_a2["contiguous_from_1"] and conn_a2["unique"],
                     "A2 completed 1..N exactly once after rollback+resume")
        check.expect(set(conn_a2["versions"]) == {"v2"},
                     "A2 acked only by the surviving old version",
                     str(conn_a2["versions"]))

        print("== scenario 6: half-line preserved across pause + upgrade ==")
        raw = socket.create_connection(("127.0.0.1", port_tcp), timeout=10)
        peer_raw = f"127.0.0.1:{raw.getsockname()[1]}"

        def line(rid: int) -> bytes:
            return (json.dumps({"id": rid, "sensor": "raw-half",
                                "temp_c": -17.5, "ts": time.time()}) + "\n"
                    ).encode()

        for rid in range(1, 6):
            raw.sendall(line(rid))
        wait_peer_acks(base, peer_raw, 5, timeout=10)
        half = line(6)
        cut = len(half) // 2
        raw.sendall(half[:cut])  # partial line, no newline
        time.sleep(0.8)          # collector reads it into its buffer
        r = http("POST", base + "/api/connections/pause", {"peer": peer_raw})
        check.expect(r.get("ok") is True, "pause with half-line in flight")
        ev = wait_event(base, "conn_paused", peer=peer_raw)
        check.expect(ev.get("leftover_bytes", 0) == cut,
                     "half-line already read is preserved in the buffer",
                     f"leftover={ev.get('leftover_bytes')} want={cut}")
        up = http("POST", base + "/api/upgrade",
                  {"new_version": "v4", "simulate_fail": False})
        check.expect(up.get("ok") is True, "upgrade v2->v4 accepted", str(up))
        wait_event(base, "upgrade_completed", new_version="v4")
        wait_conn(base, peer_raw, paused=True)
        n_raw, _ = peer_acks(base, peer_raw)
        check.expect(n_raw == 5, "no ack for the half line while paused",
                     f"acks={n_raw}")
        r = http("POST", base + "/api/connections/resume", {"peer": peer_raw})
        check.expect(r.get("ok") is True, "resume accepted")
        time.sleep(0.3)
        raw.sendall(half[cut:])  # complete line 6 on the same connection
        for rid in range(7, 13):
            raw.sendall(line(rid))
        conn_raw = wait_peer_acks(base, peer_raw, 12)
        check.expect(conn_raw["contiguous_from_1"] and conn_raw["unique"],
                     "half-line completed on the same connection, 1..12 once",
                     str(conn_raw["ids"]))
        check.expect(conn_raw["versions"].get("v2", {}).get("last_id") == 5
                     and conn_raw["versions"].get("v4", {}).get("first_id") == 6
                     and conn_raw["versions"].get("v4", {}).get("last_id") == 12,
                     "clean boundary: v2 acked 1..5, v4 acked 6..12",
                     str(conn_raw["versions"]))

        print("== scenario 7: pause during handover -> retry hint ==")
        up_result: list[dict] = []

        def do_upgrade() -> None:
            up_result.append(http("POST", base + "/api/upgrade",
                                  {"new_version": "v5", "simulate_fail": False}))

        t = threading.Thread(target=do_upgrade)
        t.start()
        # Wait until the handover flag is actually up, then the pause must
        # be rejected with a retry hint (409 + retry=true).
        deadline = time.monotonic() + 10
        handover_seen = False
        while time.monotonic() < deadline:
            if http("GET", base + "/api/status")["handover_in_progress"]:
                handover_seen = True
                break
            time.sleep(0.01)
        check.expect(handover_seen, "handover window observed")
        attempt = http("POST", base + "/api/connections/pause",
                       {"peer": peer_raw})
        check.expect(attempt.get("ok") is False and attempt.get("retry"),
                     "pause during handover rejected with retry hint",
                     str(attempt))
        t.join(timeout=15)
        check.expect(bool(up_result) and up_result[0].get("ok") is True,
                     "concurrent upgrade itself succeeded", str(up_result))
        wait_event(base, "upgrade_completed", new_version="v5")
        r = http("POST", base + "/api/connections/pause", {"peer": peer_raw})
        check.expect(r.get("ok") is True,
                     "retry after handover succeeds", str(r))
        wait_conn(base, peer_raw, paused=True)
        r = http("POST", base + "/api/connections/resume", {"peer": peer_raw})
        check.expect(r.get("ok") is True, "resume after handover works")
        for rid in range(13, 16):
            raw.sendall(line(rid))
        conn_raw = wait_peer_acks(base, peer_raw, 15)
        check.expect(conn_raw["contiguous_from_1"] and conn_raw["unique"],
                     "stream still exact after pause-across-upgrade to v5",
                     str(conn_raw["ids"]))

        print("== scenario 8: error paths ==")
        e1 = http("POST", base + "/api/connections/pause",
                  {"peer": "127.0.0.1:1"})
        check.expect(e1.get("ok") is False, "pause of unknown peer rejected",
                     str(e1))
        e2 = http("POST", base + "/api/connections/resume",
                  {"peer": peer_raw})
        check.expect(e2.get("ok") is False,
                     "resume of a non-paused connection rejected", str(e2))
        http("POST", base + "/api/connections/pause", {"peer": peer_raw})
        wait_conn(base, peer_raw, paused=True)
        e3 = http("POST", base + "/api/connections/pause", {"peer": peer_raw})
        check.expect(e3.get("ok") is False, "double pause rejected", str(e3))
        http("POST", base + "/api/connections/resume", {"peer": peer_raw})
        wait_conn(base, peer_raw, paused=False)
        raw.close()

        v = http("GET", base + "/api/verify")
        check.expect(not v["duplicate_acks"],
                     "no duplicate acks across the whole run",
                     str(v["duplicate_acks"]))
        check.expect(v.get("ok") is True,
                     "independent verification passes for every connection")
    finally:
        proc.terminate()
        try:
            gw_out, _ = proc.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            gw_out, _ = proc.communicate()

    print()
    print(f"result: {check.passed} passed, {len(check.failed)} failed")
    if check.failed:
        for f in check.failed:
            print("  -", f)
        print("\n--- gateway output tail ---")
        print("\n".join(gw_out.splitlines()[-40:]))
        return 1
    print("ALL PAUSE/RESUME CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
