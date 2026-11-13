"""End-to-end tests for the adjudication service.

Run with:  pytest -q   (from the repository root, using requirements-dev.txt)
"""

from __future__ import annotations

import concurrent.futures
import os
import tempfile
import threading
import time

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_path = str(tmp_path / "test_er.db")
    monkeypatch.setenv("ER_DB_PATH", db_path)
    # Import after the env var is set so module-level DEFAULT_DB_PATH picks it
    # up, then (re)initialise the schema at the chosen location.
    import importlib

    import app.db as db_mod
    import app.main as main_mod
    importlib.reload(db_mod)
    importlib.reload(main_mod)
    db_mod.initialize(db_path)
    with TestClient(main_mod.app) as c:
        yield c


def _import(client, rows: list[tuple[str, str, str]]):
    csv_text = "cohort,source_id,alias\n" + "\n".join(
        ",".join(r) for r in rows
    )
    resp = client.post("/api/import", json={"raw": csv_text})
    assert resp.status_code == 200, resp.text
    return resp.json()


def _adj(client, a, b, decision):
    return client.post("/api/adjudicate",
                       json={"record_a": a, "record_b": b, "decision": decision})


def _group_map(client):
    rows = client.get("/api/records").json()
    return {r["id"]: r["group_id"] for r in rows}


# --------------------------------------------------------------------------- #

def test_import_is_append_only_and_idempotent(client):
    r1 = _import(client, [("C1", "s1", "甲"), ("C1", "s2", "乙")])
    assert r1["inserted"] == 2
    # Re-import: natural key (cohort, source_id) means rows are skipped, not
    # overwritten; nothing in the original records changes.
    r2 = _import(client, [("C1", "s1", "甲-改名"), ("C1", "s3", "丙")])
    assert r2["inserted"] == 1
    assert r2["skipped_existing"] == 1
    records = {r["source_id"]: r for r in client.get("/api/records").json()}
    assert records["s1"]["alias"] == "甲"  # original row untouched
    assert len(records) == 3
    # Every record starts in its own singleton group.
    groups = {g["group_id"] for g in client.get("/api/groups").json()}
    assert len(groups) == 3


def test_same_merge_and_transitive_grouping(client):
    _import(client, [("C1", "a", "甲"), ("C2", "b", "乙"), ("C3", "c", "丙")])
    assert _adj(client, 1, 2, "same").status_code == 200
    assert _adj(client, 2, 3, "same").status_code == 200
    gm = _group_map(client)
    assert gm[1] == gm[2] == gm[3]
    # The same-person path is visible through the read-only path endpoint.
    path = client.post("/api/same-path",
                       json={"record_a": 1, "record_b": 3}).json()
    assert path["connected"] is True
    assert [n["id"] for n in path["path"]] == [1, 2, 3]


def test_one_record_per_cohort_per_group_conflict(client):
    # Two C1 records can never end up in one group, even transitively.
    _import(client, [("C1", "a", "甲"), ("C1", "b", "乙"), ("C2", "x", "X")])
    _adj(client, 1, 3, "same")
    resp = _adj(client, 2, 3, "same")  # would pull a second C1 into the group
    assert resp.status_code == 409
    body = resp.json()
    assert body["code"] == "merge_conflict"
    assert body["cohort_conflicts"][0]["cohort"] == "C1"
    # Whole-request rejection: groups remain exactly as before.
    gm = _group_map(client)
    assert gm[1] == gm[3] and gm[2] != gm[1]


def test_different_conflicts_with_same_path_and_blocks_merge(client):
    _import(client, [("C1", "a", "甲"), ("C2", "b", "乙"), ("C3", "c", "丙")])
    _adj(client, 1, 2, "same")
    # different while a same-path exists -> rejected with the path shown.
    resp = _adj(client, 1, 2, "different")
    assert resp.status_code == 409
    # An explicit active same edge already governs this pair.
    assert resp.json()["code"] == "pair_already_adjudicated"

    # A transitive-only link (no direct edge on this pair) surfaces the path.
    _adj(client, 2, 3, "same")  # 1-2-3 now one component
    resp = _adj(client, 1, 3, "different")
    assert resp.status_code == 409
    assert resp.json()["code"] == "already_same_path"
    assert [n["id"] for n in resp.json()["same_path"]] == [1, 2, 3]

    # A different-edge barrier between groups, then an attempted merge across
    # it. Use a fresh fourth record so no explicit edge pre-exists.
    _import(client, [("C4", "d", "丁")])
    assert _adj(client, 3, 4, "different").status_code == 200
    resp = _adj(client, 1, 4, "same")
    assert resp.status_code == 409
    body = resp.json()
    assert body["code"] == "merge_conflict"
    assert len(body["different_conflicts"]) == 1
    assert {body["different_conflicts"][0]["left"]["id"],
            body["different_conflicts"][0]["right"]["id"]} == {3, 4}
    # State preserved.
    gm = _group_map(client)
    assert gm[1] == gm[2] == gm[3] and gm[4] != gm[1]


def test_revoke_splits_groups_and_allows_re_adjudication(client):
    _import(client, [("C1", "a", "甲"), ("C2", "b", "乙"), ("C3", "c", "丙")])
    _adj(client, 1, 2, "same")
    _adj(client, 2, 3, "same")
    assert len(set(_group_map(client).values())) == 1

    adj_id = client.get("/api/adjudications").json()[0]["id"]  # the 1-2 edge
    resp = client.post("/api/revoke", json={"adjudication_id": adj_id})
    assert resp.status_code == 200
    body = resp.json()
    assert body["groups_after"] == 2  # {1} and {2,3} after the split

    gm = _group_map(client)
    assert gm[2] == gm[3] and gm[1] != gm[2]

    # After revocation the pair can be adjudicated again (history row retained).
    assert _adj(client, 1, 2, "different").status_code == 200
    history = client.get("/api/adjudications?active_only=false").json()
    revoked = [a for a in history if not a["active"]]
    assert len(revoked) == 1 and revoked[0]["revoked_at"]
    # Now merging across the fresh different barrier is rejected.
    assert _adj(client, 1, 3, "same").status_code == 409


def test_duplicate_and_flipped_decisions(client):
    _import(client, [("C1", "a", "甲"), ("C2", "b", "乙")])
    r1 = _adj(client, 1, 2, "same")
    r2 = _adj(client, 2, 1, "same")  # reversed pair, same decision -> no-op
    assert r1.status_code == r2.status_code == 200
    assert r2.json()["status"] == "noop"
    flip = _adj(client, 1, 2, "different")
    assert flip.status_code == 409
    assert flip.json()["code"] == "pair_already_adjudicated"


def test_export_refects_one_committed_snapshot(client):
    _import(client, [("C1", "a", "甲"), ("C2", "b", "乙")])
    _adj(client, 1, 2, "same")
    resp = client.get("/api/export")
    assert resp.status_code == 200
    assert "attachment" in resp.headers["content-disposition"]
    lines = resp.text.strip().splitlines()
    assert lines[0] == "cohort,source_id,alias,group_id"
    groups = {ln.split(",")[-1] for ln in lines[1:]}
    assert len(groups) == 1  # both rows map to the same committed group


def test_failed_adjudication_prior_state_preserved(client):
    _import(client, [("C1", "a", "甲"), ("C1", "b", "乙"), ("C2", "x", "X")])
    _adj(client, 1, 3, "same")
    before = _group_map(client)
    stats_before = client.get("/api/stats").json()
    bad = _adj(client, 2, 3, "same")
    assert bad.status_code == 409
    assert _group_map(client) == before
    assert client.get("/api/stats").json() == stats_before
    # No active adjudication row was left behind by the rolled-back request.
    assert client.get("/api/adjudications").json() and all(
        a["decision"] == "same" for a in client.get("/api/adjudications").json())


def test_concurrent_independent_adjudications_both_commit(client):
    # Two disjoint pairs on separate processes/threads must both succeed.
    _import(client, [("C1", "a", "甲"), ("C2", "b", "乙"),
                     ("C3", "c", "丙"), ("C4", "d", "丁")])
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futs = [pool.submit(_adj, client, 1, 2, "same"),
                pool.submit(_adj, client, 3, 4, "same")]
        results = [f.result().status_code for f in futs]
    assert sorted(results) == [200, 200]
    gm = _group_map(client)
    assert gm[1] == gm[2] and gm[3] == gm[4] and gm[1] != gm[3]


def test_concurrent_conflicting_merges_cannot_both_win(client):
    # The requirement: two decisions may each be legal on their own but be
    # illegal once combined, and the combination must never commit.
    #
    # Layout: 1(C1), 2(C2), 3(C3) start as singleton groups.
    #   * 1 vs 3 is first adjudicated "different" (a cross-group barrier);
    #   * T1 then asks 1~2 same -- legal on the committed snapshot;
    #   * T2 then asks 2~3 same -- legal on the committed snapshot;
    #   * both together would connect the "different" pair 1...3.
    # Serialised by BEGIN IMMEDIATE, exactly one merge commits and the other
    # is rejected as a whole-request 409; the barrier is never violated.
    _import(client, [("C1", "a", "甲"), ("C2", "b", "乙"), ("C3", "c", "丙")])
    assert _adj(client, 1, 3, "different").status_code == 200

    barrier = __import__("threading").Barrier(2)

    def racing(a, b):
        barrier.wait()  # maximise overlap
        return _adj(client, a, b, "same")

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futs = [pool.submit(racing, 1, 2), pool.submit(racing, 2, 3)]
        statuses = sorted(f.result().status_code for f in futs)
    assert statuses == [200, 409]

    # The committed state still honours every invariant: records 1 and 3
    # remain in distinct groups.
    gm = _group_map(client)
    assert gm[1] != gm[3]
    groups = client.get("/api/groups").json()
    for g in groups:
        cohorts = [m["cohort"] for m in g["members"]]
        assert len(cohorts) == len(set(cohorts))


def test_concurrent_cohort_conflict_merges(client):
    # Cohort-uniqueness shape of the "individually legal, jointly illegal"
    # guarantee. Records 1(C1) and 2(C1) are two records of the same cohort;
    # record 3(C2) and record 4(C2) sit in a second cohort.
    #
    #   T1 asks 1~3 same, T2 asks 2~4 same -- both legal standalone and they
    #   produce two disjoint groups. Then two racing follow-ups, each legal
    #   against the current snapshot, try to join those groups:
    #     F1: 1~4 same   F2: 2~3 same
    #   Either follow-up alone is fine; both together would leave one group
    #   containing two C1 and two C2 records. Serialisation by BEGIN IMMEDIATE
    #   must commit one and reject the other as a whole-request conflict.
    _import(client, [("C1", "a", "甲"), ("C1", "b", "乙"),
                     ("C2", "x", "X"), ("C2", "y", "Y")])
    assert _adj(client, 1, 3, "same").status_code == 200  # group X
    assert _adj(client, 2, 4, "same").status_code == 200  # group Y

    barrier = __import__("threading").Barrier(2)

    def racing(a, b):
        barrier.wait()
        return _adj(client, a, b, "same")

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futs = [pool.submit(racing, 1, 4), pool.submit(racing, 2, 3)]
        results = [f.result() for f in futs]
    statuses = sorted(r.status_code for r in results)
    # Every losing request is a genuine constraint rejection (never a lock
    # error); the combined schedule may not produce an invalid partition.
    for r in results:
        if r.status_code == 409:
            assert r.json()["code"] == "merge_conflict"
    assert 409 in statuses

    # Committed partition invariants, regardless of which interleaving won:
    # no group holds two records of one cohort, and the two C1 records never
    # share a group.
    groups = client.get("/api/groups").json()
    for g in groups:
        cohorts = [m["cohort"] for m in g["members"]]
        assert len(cohorts) == len(set(cohorts))
    gm = _group_map(client)
    assert gm[1] != gm[2]
    assert gm[3] != gm[4]


def test_concurrent_different_vs_same_on_same_pair(client):
    _import(client, [("C1", "a", "甲"), ("C2", "b", "乙")])
    barrier = __import__("threading").Barrier(2)

    def racing(decision):
        barrier.wait()
        return _adj(client, 1, 2, decision)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futs = [pool.submit(racing, "same"), pool.submit(racing, "different")]
        results = [f.result() for f in futs]
    # Unique partial index + IMMEDIATE serialisation: one decision commits,
    # the other is rejected (pair already adjudicated) rather than duplicated.
    assert sorted(r.status_code for r in results) == [200, 409]
    active = client.get("/api/adjudications").json()
    assert len(active) == 1


# --------------------------------------------------------------------------- #
# Lock-free computation: the fix under test
# --------------------------------------------------------------------------- #

def _paused_call(client, path, payload, outcome, key="resp"):
    """Fire a request whose computation phase is paused (demo hook)."""
    outcome[key] = client.post(path, json={**payload, "demo_pause_seconds": 2.5})


def test_computation_phase_does_not_hold_write_lock(client):
    # The core fix: while one request is inside its (paused) relationship
    # computation, an unrelated adjudication from the same or another entry
    # point must commit immediately instead of waiting for the write lock.
    _import(client, [("C1", "a", "甲"), ("C2", "b", "乙"),
                     ("C3", "c", "丙"), ("C4", "d", "丁")])
    outcome = {}
    t = threading.Thread(target=_paused_call,
                         args=(client, "/api/adjudicate",
                               {"record_a": 1, "record_b": 2, "decision": "same"},
                               outcome))
    t.start()
    time.sleep(0.6)  # let the first request settle into its computation pause

    start = time.monotonic()
    resp = _adj(client, 3, 4, "same")  # unrelated pair: must not wait
    elapsed = time.monotonic() - start
    t.join(timeout=15)

    assert resp.status_code == 200
    assert elapsed < 2.0, f"无关裁决等待了 {elapsed:.2f}s——计算期间仍占着写锁"
    assert outcome["resp"].status_code == 200
    # The paused request recomputed on the newer snapshot and committed too:
    # neither adjudication was overwritten or lost.
    gm = _group_map(client)
    assert gm[1] == gm[2] and gm[3] == gm[4] and gm[1] != gm[3]
    assert len(client.get("/api/adjudications").json()) == 2


def test_interleaved_conflicting_adjudication_still_rejected(client):
    # Individually legal, jointly illegal — now with the loser computing on a
    # stale snapshot: the version check must force a recompute that rejects.
    _import(client, [("C1", "a", "甲"), ("C2", "b", "乙"), ("C3", "c", "丙")])
    assert _adj(client, 1, 3, "different").status_code == 200

    outcome = {}
    t = threading.Thread(target=_paused_call,
                         args=(client, "/api/adjudicate",
                               {"record_a": 1, "record_b": 2, "decision": "same"},
                               outcome))
    t.start()
    time.sleep(0.6)
    resp = _adj(client, 2, 3, "same")  # commits while the other is computing
    t.join(timeout=15)

    assert resp.status_code == 200
    assert outcome["resp"].status_code == 409
    assert outcome["resp"].json()["code"] == "merge_conflict"
    # Final state honours every invariant: the 1...3 barrier is intact and
    # exactly one of the racing merges committed.
    gm = _group_map(client)
    assert gm[2] == gm[3] and gm[1] != gm[2]
    active = {(a["left_id"], a["right_id"], a["decision"])
              for a in client.get("/api/adjudications").json()}
    assert active == {(1, 3, "different"), (2, 3, "same")}


def test_import_during_paused_computation_is_not_lost(client):
    # A roster import committed while another request computes must survive:
    # the stale computation retries and its partition rewrite covers the new
    # records instead of silently ungrouping them.
    _import(client, [("C1", "a", "甲"), ("C2", "b", "乙")])
    outcome = {}
    t = threading.Thread(target=_paused_call,
                         args=(client, "/api/adjudicate",
                               {"record_a": 1, "record_b": 2, "decision": "same"},
                               outcome))
    t.start()
    time.sleep(0.6)
    imp = _import(client, [("C3", "c", "丙"), ("C4", "d", "丁")])
    t.join(timeout=15)

    assert imp["inserted"] == 2
    assert outcome["resp"].status_code == 200
    records = client.get("/api/records").json()
    assert len(records) == 4
    assert all(r["group_id"] for r in records)  # nobody lost its group
    gm = {r["id"]: r["group_id"] for r in records}
    assert gm[1] == gm[2]
    assert len({gm[1], gm[3], gm[4]}) == 3  # new records kept singleton groups


def test_revoke_during_paused_computation_keeps_both_changes(client):
    # A revocation committed during another request's computation must not be
    # clobbered by that request's stale partition rewrite, and vice versa.
    _import(client, [("C1", "a", "甲"), ("C2", "b", "乙"),
                     ("C3", "c", "丙"), ("C4", "d", "丁")])
    _adj(client, 1, 2, "same")
    _adj(client, 3, 4, "same")
    adj_id = next(a for a in client.get("/api/adjudications").json()
                  if a["left_id"] == 3)["id"]

    outcome = {}
    t = threading.Thread(target=_paused_call,
                         args=(client, "/api/revoke",
                               {"adjudication_id": adj_id}, outcome))
    t.start()
    time.sleep(0.6)
    resp = _adj(client, 2, 3, "different")  # commits while the revoke computes
    t.join(timeout=15)

    assert resp.status_code == 200
    assert outcome["resp"].status_code == 200
    gm = _group_map(client)
    assert gm[1] == gm[2]           # the untouched merge survives
    assert gm[3] != gm[4]           # the revoke's split survives
    assert gm[2] != gm[3]           # the fresh different barrier holds
    decisions = {(a["left_id"], a["right_id"]): a["decision"]
                 for a in client.get("/api/adjudications").json()}
    assert decisions == {(1, 2): "same", (2, 3): "different"}


def test_demo_gate_pauses_and_releases_computation(client):
    _import(client, [("C1", "a", "甲"), ("C2", "b", "乙")])
    assert client.get("/api/demo/gate").json()["open"] is True
    assert client.post("/api/demo/gate", json={"open": False}).json()["open"] is False
    try:
        outcome = {}
        t = threading.Thread(target=lambda: outcome.update(
            resp=_adj(client, 1, 2, "same")))
        t.start()
        t.join(timeout=1.0)
        # Parked inside the computation phase: no response, and (per the other
        # tests) no write lock held either.
        assert t.is_alive() and "resp" not in outcome
        client.post("/api/demo/gate", json={"open": True})
        t.join(timeout=15)
        assert outcome["resp"].status_code == 200
    finally:
        client.post("/api/demo/gate", json={"open": True})
    gm = _group_map(client)
    assert gm[1] == gm[2]


def test_merge_conflict_detected_regardless_of_edge_orientation(client):
    # The cross-group different-edge check must be symmetric: the barrier
    # edge (1,5) has its *left* endpoint in the second of the merged groups,
    # which a left-in-group-A query would silently miss.
    _import(client, [("C1", "a", "甲"), ("C2", "b", "乙"), ("C3", "c", "丙"),
                     ("C4", "d", "丁"), ("C5", "e", "戊")])
    assert _adj(client, 2, 5, "same").status_code == 200   # group X = {2,5}
    assert _adj(client, 1, 4, "same").status_code == 200   # group Y = {1,4}
    assert _adj(client, 1, 5, "different").status_code == 200  # 1 in Y, 5 in X
    resp = _adj(client, 2, 4, "same")  # merging X+Y would group 1 with 5
    assert resp.status_code == 409
    body = resp.json()
    assert body["code"] == "merge_conflict"
    assert len(body["different_conflicts"]) == 1
    assert {body["different_conflicts"][0]["left"]["id"],
            body["different_conflicts"][0]["right"]["id"]} == {1, 5}
    gm = _group_map(client)
    assert gm[1] != gm[5]
