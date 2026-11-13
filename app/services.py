"""Domain logic: import, adjudication, revocation, grouping, export.

Concurrency model — optimistic, computation never holds the write lock
----------------------------------------------------------------------
Graph traversal, partition recomputation and conflict-path search are the
expensive part of an adjudication, and they must *not* hold the SQLite write
lock: while one process computes, another process has to be able to commit
adjudications on unrelated records.  Every mutation therefore runs in three
phases:

1. **Snapshot** — a read-only connection (``mode=ro``, ``query_only=ON``)
   reads one consistent WAL snapshot inside a single read transaction:
   records, adjudication history, current partition and the two version
   counters in ``meta``.  No write lock is taken; WAL readers never block
   the writer.
2. **Compute** — pure in-memory planning on that snapshot: connected
   components, merge validation, conflict paths and the resulting partition
   change.  Still no lock of any kind; the demo gate / per-request pause
   hook into this phase to prove it.
3. **Commit** — a short ``BEGIN IMMEDIATE`` transaction re-reads the version
   counters.  If they moved (a concurrent adjudication, revocation or import
   committed meanwhile) the transaction is rolled back and the whole
   operation is retried against a fresh snapshot, so interleaved changes are
   never overwritten by stale results and never lost.  If the versions still
   match, the planned writes (adjudication row + partition change + version
   bump) are applied atomically: adjudication and grouping persist together,
   and any failure leaves the previously committed state untouched.

Because validation happens against the snapshot and the commit only lands
when that snapshot is still current, two individually-legal adjudications
can never combine into an invalid partition: the first commit bumps the
version, the second request recomputes on the new state and is rejected
there (HTTP 409).

Under sustained write contention the optimistic retries could in theory
starve, so after ``_MAX_ATTEMPTS`` attempts the operation falls back to
planning inside the write transaction (the pre-fix behaviour) and the
request always terminates.  The fast path — the one the demo and the tests
exercise — never holds the write lock while computing.

Read paths (group listing, export, path lookup) use read-only connections
and never acquire a write lock.
"""

from __future__ import annotations

import csv
import io
import sqlite3
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from .db import get_read_conn, write_tx


class ConflictError(Exception):
    """The requested adjudication conflicts with the current committed state.

    ``details`` is a JSON-serialisable payload describing every violated
    constraint, including the records and the same-person connection paths
    involved, so the UI can show why the whole request was rejected.
    """

    def __init__(self, message: str, details: dict):
        super().__init__(message)
        self.details = details


class _StaleSnapshot(Exception):
    """The version counters moved between snapshot and commit; retry."""


# How many optimistic attempts are made before the lock-holding fallback.
_MAX_ATTEMPTS = 8


# --------------------------------------------------------------------------- #
# Demo control: a process-local gate that pauses the computation phase
# --------------------------------------------------------------------------- #
#
# The gate exists to *demonstrate* that relationship computation holds no
# locks: close it on one replica and its adjudications park inside phase 2
# (snapshot already read, nothing locked) until it is reopened, while the
# other replica keeps committing.  It is a threading.Event because the
# FastAPI endpoints are synchronous and run in worker threads.

_compute_gate = threading.Event()
_compute_gate.set()


def set_compute_gate(open_: bool) -> None:
    """Open/close the computation gate *of this process* (demo control)."""
    if open_:
        _compute_gate.set()
    else:
        _compute_gate.clear()


def compute_gate_open() -> bool:
    return _compute_gate.is_set()


def _compute_pause(pause_seconds: float) -> None:
    """Hold the computation phase.  No database locks are held while waiting."""
    _compute_gate.wait()
    if pause_seconds > 0:
        time.sleep(min(pause_seconds, 30.0))


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #

def _pair(a: int, b: int) -> tuple[int, int]:
    """Normalise an unordered record pair (smaller id first)."""
    return (a, b) if a < b else (b, a)


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _get_record(conn: sqlite3.Connection, record_id: int) -> sqlite3.Row:
    row = conn.execute(
        "SELECT id, cohort, source_id, alias FROM records WHERE id = ?",
        (record_id,),
    ).fetchone()
    if row is None:
        raise ConflictError(f"记录 {record_id} 不存在", {"code": "record_not_found",
                                                     "record_id": record_id})
    return row


def _bfs_path(graph: dict[int, set[int]], src: int, dst: int) -> Optional[list[int]]:
    """Shortest path in the same-person graph; ``None`` when disconnected."""
    if src == dst:
        return [src]
    seen = {src}
    queue: deque[tuple[int, list[int]]] = deque([(src, [src])])
    while queue:
        node, path = queue.popleft()
        for nxt in graph.get(node, ()):
            if nxt in seen:
                continue
            new_path = path + [nxt]
            if nxt == dst:
                return new_path
            seen.add(nxt)
            queue.append((nxt, new_path))
    return None


def _components(record_ids, graph: dict[int, set[int]]):
    """Connected components of the same-person graph over all records."""
    comp_of: dict[int, int] = {}
    components: list[list[int]] = []
    for start in sorted(record_ids):
        if start in comp_of:
            continue
        idx = len(components)
        comp: list[int] = []
        queue: deque[int] = deque([start])
        comp_of[start] = idx
        while queue:
            node = queue.popleft()
            comp.append(node)
            for nxt in graph.get(node, ()):
                if nxt not in comp_of:
                    comp_of[nxt] = idx
                    queue.append(nxt)
        components.append(comp)
    return comp_of, components


def _record_brief(row) -> dict:
    return {"id": row["id"], "cohort": row["cohort"],
            "source_id": row["source_id"], "alias": row["alias"]}


# --------------------------------------------------------------------------- #
# Snapshot: one consistent committed view, read without any write lock
# --------------------------------------------------------------------------- #

class _Snapshot:
    """Everything the planners need, captured from one committed read transaction."""

    def __init__(self, *, records_version: int, edges_version: int,
                 records: list[dict], adjudications: list[dict],
                 memberships: list[dict], group_ids: list[int]):
        self.records_version = records_version
        self.edges_version = edges_version
        self.records = {r["id"]: r for r in records}
        self.adjudications = adjudications  # full history incl. revoked rows
        self.record_group = {m["record_id"]: m["group_id"] for m in memberships}
        groups: dict[int, set[int]] = {gid: set() for gid in group_ids}
        for m in memberships:
            groups.setdefault(m["group_id"], set()).add(m["record_id"])
        self.group_members = groups
        missing = [rid for rid in self.records if rid not in self.record_group]
        if missing:  # every committed record must sit in exactly one group
            raise ConflictError("记录未分配分组，数据不一致",
                                {"code": "inconsistent_state",
                                 "record_id": missing[0]})
        # Derived: active same-edge graph and its connected components.
        self.graph: dict[int, set[int]] = {}
        for row in adjudications:
            if row["active"] and row["decision"] == "same":
                self.graph.setdefault(row["left_id"], set()).add(row["right_id"])
                self.graph.setdefault(row["right_id"], set()).add(row["left_id"])
        self.comp_of, self.components = _components(self.records.keys(), self.graph)

    def active_decision(self, left: int, right: int) -> Optional[str]:
        for row in self.adjudications:
            if row["active"] and row["left_id"] == left and row["right_id"] == right:
                return row["decision"]
        return None

    def adjudication(self, adjudication_id: int) -> Optional[dict]:
        for row in self.adjudications:
            if row["id"] == adjudication_id:
                return row
        return None


def _load_snapshot(conn: sqlite3.Connection | None = None) -> _Snapshot:
    """Read one consistent committed snapshot.

    Without an argument a short-lived read-only connection is used and all
    reads run inside a single read transaction (WAL pins the snapshot; the
    writer in another process is never blocked).  Pass an open write
    connection to snapshot inside its transaction (contention fallback).
    """
    own = conn is None
    if own:
        conn = get_read_conn()
        conn.isolation_level = None  # explicit transaction control below
        conn.execute("BEGIN")
    try:
        meta = conn.execute(
            "SELECT records_version, edges_version FROM meta WHERE id = 1"
        ).fetchone()
        records = [dict(r) for r in conn.execute(
            "SELECT id, cohort, source_id, alias FROM records ORDER BY id")]
        adjudications = [dict(r) for r in conn.execute(
            "SELECT id, left_id, right_id, decision, active"
            " FROM adjudications ORDER BY id")]
        memberships = [dict(r) for r in conn.execute(
            "SELECT record_id, group_id FROM record_groups")]
        group_ids = [r["group_id"] for r in conn.execute(
            "SELECT group_id FROM groups_t")]
        snap = _Snapshot(
            records_version=meta["records_version"],
            edges_version=meta["edges_version"],
            records=records, adjudications=adjudications,
            memberships=memberships, group_ids=group_ids)
        if own:
            conn.execute("COMMIT")
        return snap
    except Exception:
        if own:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
        raise
    finally:
        if own:
            conn.close()


# --------------------------------------------------------------------------- #
# Plans: the pure computation phase (no database access, no locks)
# --------------------------------------------------------------------------- #

@dataclass
class _PartitionChange:
    """Difference between the committed partition and the recomputed one."""
    assignment: dict[int, int]        # record_id -> group_id, every record
    changed: dict[int, int]           # the subset whose group actually moves
    new_group_ids: list[int]          # group ids to create
    vanished_group_ids: list[int]     # group ids to delete


@dataclass
class _Plan:
    """A fully computed mutation, ready to be applied in one short commit."""
    result: dict
    noop: bool = False                # nothing to write (idempotent request)
    check_records: bool = True        # commit requires records_version stable
    check_edges: bool = True          # commit requires edges_version stable
    bump_records: bool = False
    bump_edges: bool = False
    adjudication_insert: Optional[tuple[int, int, str]] = None
    adjudication_revoke: Optional[int] = None
    record_inserts: list[tuple[str, str, str]] = field(default_factory=list)
    partition: Optional[_PartitionChange] = None


def _partition_change(snap: _Snapshot, new_components: list[list[int]]) -> _PartitionChange:
    """Map recomputed components onto group ids.

    A component identical to an existing group keeps that group's id, so
    untouched parts of the partition stay stable; changed components receive
    fresh ids in deterministic order.
    """
    existing_sets = {frozenset(members): gid
                     for gid, members in snap.group_members.items()}
    next_id = max(snap.group_members.keys(), default=0) + 1
    assignment: dict[int, int] = {}
    used: set[int] = set()
    for comp in sorted(new_components, key=lambda c: min(c)):
        gid = existing_sets.get(frozenset(comp))
        if gid is None or gid in used:
            while next_id in used:
                next_id += 1
            gid = next_id
            next_id += 1
        used.add(gid)
        for rid in comp:
            assignment[rid] = gid
    changed = {rid: gid for rid, gid in assignment.items()
               if snap.record_group.get(rid) != gid}
    vanished = [gid for gid in snap.group_members if gid not in used]
    new_ids = sorted(g for g in used if g not in snap.group_members)
    return _PartitionChange(assignment=assignment, changed=changed,
                            new_group_ids=new_ids, vanished_group_ids=vanished)


def _plan_adjudicate(snap: _Snapshot, left: int, right: int, decision: str) -> _Plan:
    rec_left = snap.records.get(left)
    rec_right = snap.records.get(right)
    if rec_left is None:
        raise ConflictError(f"记录 {left} 不存在", {"code": "record_not_found",
                                                    "record_id": left})
    if rec_right is None:
        raise ConflictError(f"记录 {right} 不存在", {"code": "record_not_found",
                                                     "record_id": right})

    existing = snap.active_decision(left, right)
    if existing is not None:
        if existing == decision:
            return _Plan(noop=True,
                         result={"status": "noop", "message": "该裁决已存在，状态未改变",
                                 "left_id": left, "right_id": right, "decision": decision})
        raise ConflictError(
            f"该记录对已有生效裁决（{existing}），不能改为 {decision}；请先撤销原裁决",
            {"code": "pair_already_adjudicated", "left_id": left,
             "right_id": right, "existing": existing},
        )

    if decision == "same":
        return _plan_same(snap, left, right, rec_left, rec_right)
    return _plan_different(snap, left, right, rec_left, rec_right)


def _plan_same(snap: _Snapshot, left: int, right: int,
               rec_left: dict, rec_right: dict) -> _Plan:
    idx_a, idx_b = snap.comp_of[left], snap.comp_of[right]
    if idx_a == idx_b:
        # Already transitively linked: the edge simply records the decision.
        return _Plan(result={"status": "added_edge_within_group",
                             "message": "两条记录已在同一分组，裁决作为显式同人边保存"},
                     adjudication_insert=(left, right, "same"), bump_edges=True)

    comp_a = snap.components[idx_a]
    comp_b = snap.components[idx_b]

    # Constraint 1: at most one record per cohort inside the merged group.
    cohorts: dict[str, list[dict]] = {}
    for rid in comp_a:
        cohorts.setdefault(snap.records[rid]["cohort"], []).append(snap.records[rid])
    cohort_conflicts = []
    for rid in comp_b:
        rec = snap.records[rid]
        if rec["cohort"] in cohorts:
            cohort_conflicts.append({
                "cohort": rec["cohort"],
                "records": [_record_brief(x) for x in cohorts[rec["cohort"]]]
                           + [_record_brief(rec)],
            })

    # Constraint 2: no active "different" edge between the two components.
    # The check is symmetric in the edge endpoints: a stored edge (l, r) may
    # have l in either component.
    different_conflicts = []
    for edge in snap.adjudications:
        if not (edge["active"] and edge["decision"] == "different"):
            continue
        l, r = edge["left_id"], edge["right_id"]
        if {snap.comp_of.get(l), snap.comp_of.get(r)} != {idx_a, idx_b}:
            continue
        different_conflicts.append({
            "left": _record_brief(snap.records[l]),
            "right": _record_brief(snap.records[r]),
            # Show how each endpoint reaches the two records being merged.
            "path_to_a": _bfs_path(snap.graph, l, left)
                         or _bfs_path(snap.graph, l, right),
            "path_to_b": _bfs_path(snap.graph, r, right)
                         or _bfs_path(snap.graph, r, left),
        })

    if cohort_conflicts or different_conflicts:
        raise ConflictError(
            "合并将违反分组约束，整次裁决已拒绝",
            {
                "code": "merge_conflict",
                "cohort_conflicts": cohort_conflicts,
                "different_conflicts": different_conflicts,
                "record_a": _record_brief(rec_left),
                "record_b": _record_brief(rec_right),
            },
        )

    # Merge the two components; every other component is carried over.
    merged = sorted(set(comp_a) | set(comp_b))
    new_components = [c for i, c in enumerate(snap.components)
                      if i != idx_a and i != idx_b]
    new_components.append(merged)
    partition = _partition_change(snap, new_components)
    return _Plan(result={"status": "merged", "message": "同人裁决生效，两个分组合并",
                         "merged_group_id": partition.assignment[left]},
                 adjudication_insert=(left, right, "same"),
                 partition=partition, bump_edges=True)


def _plan_different(snap: _Snapshot, left: int, right: int,
                    rec_left: dict, rec_right: dict) -> _Plan:
    if snap.comp_of[left] == snap.comp_of[right]:
        # Same component means a transitive "same" path exists; locate it for
        # the error message.
        path = _bfs_path(snap.graph, left, right)
        path_records = [_record_brief(snap.records[rid])
                        for rid in (path or [left, right])]
        raise ConflictError(
            "两条记录已通过同人关系连接，不能判为不同人",
            {"code": "already_same_path",
             "record_a": _record_brief(rec_left),
             "record_b": _record_brief(rec_right),
             "same_path": path_records},
        )

    # Different components: the edge is sufficient; the partition is unchanged.
    return _Plan(result={"status": "different_edge",
                         "message": "不同人裁决生效，两个分组保持独立并被标记互斥"},
                 adjudication_insert=(left, right, "different"), bump_edges=True)


def _plan_revoke(snap: _Snapshot, adjudication_id: int) -> _Plan:
    row = snap.adjudication(adjudication_id)
    if row is None:
        raise ConflictError(f"裁决 {adjudication_id} 不存在",
                            {"code": "adjudication_not_found",
                             "adjudication_id": adjudication_id})
    if not row["active"]:
        return _Plan(noop=True,
                     result={"status": "noop", "message": "该裁决已撤销，状态未改变",
                             "adjudication_id": adjudication_id})

    # Rebuild the partition purely from the remaining active "same" edges, so
    # removing an edge may split a group into several connected components.
    graph: dict[int, set[int]] = {}
    for e in snap.adjudications:
        if e["active"] and e["decision"] == "same" and e["id"] != adjudication_id:
            graph.setdefault(e["left_id"], set()).add(e["right_id"])
            graph.setdefault(e["right_id"], set()).add(e["left_id"])
    comp_of, components = _components(snap.records.keys(), graph)

    # Re-validate: no active different edge may end up inside one component.
    violations = [(e["left_id"], e["right_id"]) for e in snap.adjudications
                  if e["active"] and e["decision"] == "different"
                  and comp_of[e["left_id"]] == comp_of[e["right_id"]]]
    if violations:
        # Should be impossible given adjudication validation; guard anyway so
        # that a broken invariant is never silently committed.
        raise ConflictError("撤销后重新分组发现不同人记录落入同组，操作已回滚",
                            {"code": "invariant_violation_after_rebuild",
                             "violating_pairs": violations})

    partition = _partition_change(snap, components)
    return _Plan(result={"status": "revoked",
                         "message": "裁决已撤销并重新计算分组",
                         "adjudication_id": adjudication_id,
                         "groups_before": len(snap.group_members),
                         "groups_after": len(components),
                         "group_count": len(components),
                         "component_sizes": [len(c) for c in components]},
                 adjudication_revoke=adjudication_id,
                 partition=partition, bump_edges=True)


def _plan_import(snap: _Snapshot, rows: list[tuple[str, str, str]], *,
                 duplicate_in_file: int, line_errors: list,
                 total_rows_read: int) -> _Plan:
    existing = {(r["cohort"], r["source_id"]) for r in snap.records.values()}
    to_insert = [row for row in rows if (row[0], row[1]) not in existing]
    result = {"inserted": len(to_insert),
              "skipped_existing": len(rows) - len(to_insert),
              "duplicate_in_file": duplicate_in_file,
              "line_errors": line_errors,
              "total_rows_read": total_rows_read}
    if not to_insert:
        return _Plan(noop=True, result=result)
    # Imports only touch `records` (+ singleton groups), so only the records
    # version needs to be stable / bumped; concurrent adjudications neither
    # invalidate nor are invalidated by an import plan.
    return _Plan(result=result, record_inserts=to_insert,
                 check_edges=False, bump_records=True)


# --------------------------------------------------------------------------- #
# Commit: version-checked application of a computed plan
# --------------------------------------------------------------------------- #

def _apply_plan(conn: sqlite3.Connection, snap: _Snapshot, plan: _Plan) -> None:
    """Apply ``plan`` inside the current write transaction.

    Raises :class:`_StaleSnapshot` when the snapshot the plan was computed
    from is no longer current; the caller rolls back and recomputes.
    """
    meta = conn.execute(
        "SELECT records_version, edges_version FROM meta WHERE id = 1"
    ).fetchone()
    if plan.check_records and meta["records_version"] != snap.records_version:
        raise _StaleSnapshot(
            f"records_version moved {snap.records_version} -> {meta['records_version']}")
    if plan.check_edges and meta["edges_version"] != snap.edges_version:
        raise _StaleSnapshot(
            f"edges_version moved {snap.edges_version} -> {meta['edges_version']}")

    if plan.adjudication_insert is not None:
        left, right, decision = plan.adjudication_insert
        conn.execute(
            "INSERT INTO adjudications (left_id, right_id, decision, active, created_at)"
            " VALUES (?, ?, ?, 1, ?)",
            (left, right, decision, _now()),
        )
    if plan.adjudication_revoke is not None:
        cur = conn.execute(
            "UPDATE adjudications SET active = 0, revoked_at = ?"
            " WHERE id = ? AND active = 1",
            (_now(), plan.adjudication_revoke),
        )
        if cur.rowcount != 1:  # concurrently revoked; version bump catches it
            raise _StaleSnapshot("adjudication revoked by a concurrent commit")

    for cohort, source_id, alias in plan.record_inserts:
        cur = conn.execute(
            "INSERT INTO records (cohort, source_id, alias) VALUES (?, ?, ?)",
            (cohort, source_id, alias),
        )
        new_id = cur.lastrowid
        group_id = conn.execute("INSERT INTO groups_t DEFAULT VALUES").lastrowid
        conn.execute(
            "INSERT INTO record_groups (record_id, group_id) VALUES (?, ?)",
            (new_id, group_id),
        )

    if plan.partition is not None:
        p = plan.partition
        # Order matters for the immediate foreign key record_groups -> groups_t.
        for rid in sorted(p.changed):
            conn.execute("DELETE FROM record_groups WHERE record_id = ?", (rid,))
        for gid in p.vanished_group_ids:
            conn.execute("DELETE FROM groups_t WHERE group_id = ?", (gid,))
        for gid in p.new_group_ids:
            conn.execute("INSERT INTO groups_t (group_id) VALUES (?)", (gid,))
        for rid, gid in sorted(p.changed.items()):
            conn.execute(
                "INSERT INTO record_groups (record_id, group_id) VALUES (?, ?)",
                (rid, gid),
            )

    if plan.bump_records:
        conn.execute("UPDATE meta SET records_version = records_version + 1"
                     " WHERE id = 1")
    if plan.bump_edges:
        conn.execute("UPDATE meta SET edges_version = edges_version + 1"
                     " WHERE id = 1")


def _run_optimistic(plan_fn, *, pause_seconds: float = 0.0,
                    use_gate: bool = False) -> dict:
    """Run ``plan_fn(snapshot)`` optimistically until its commit lands.

    The plan is computed on a lock-free snapshot; the commit re-checks the
    version counters and, on any interleaved change, the plan is recomputed
    on a fresh snapshot instead of overwriting or losing the newer work.
    """
    paused = False
    for attempt in range(_MAX_ATTEMPTS):
        snap = _load_snapshot()
        if not paused:
            paused = True
            if use_gate:
                # Demo hook: park inside the computation phase.  No locks are
                # held here — the snapshot connection is already closed.
                _compute_pause(pause_seconds)
        plan = plan_fn(snap)
        if plan.noop:
            return plan.result
        try:
            with write_tx() as conn:
                _apply_plan(conn, snap, plan)
            return plan.result
        except (_StaleSnapshot, sqlite3.IntegrityError):
            # Someone committed in between (or raced the unique pair index):
            # recompute on a fresh snapshot.  Their change is preserved.
            time.sleep(min(0.02 * (attempt + 1), 0.2))
    # Liveness fallback for sustained write contention: plan once more inside
    # the write transaction (the pre-fix behaviour) so the request always
    # terminates.  Never taken on the normal path.
    with write_tx() as conn:
        snap = _load_snapshot(conn)
        plan = plan_fn(snap)
        if not plan.noop:
            _apply_plan(conn, snap, plan)
        return plan.result


# --------------------------------------------------------------------------- #
# Adjudication / revocation (public entry points)
# --------------------------------------------------------------------------- #

def adjudicate(record_a: int, record_b: int, decision: str,
               demo_pause_seconds: float = 0.0) -> dict:
    """Create an active adjudication between two records.

    Constraints (validated on a consistent snapshot; the commit only lands
    when that snapshot is still current, and failure rolls everything back):

    * the two records must exist and be distinct;
    * a pair may only hold one active adjudication (re-submitting the same
      decision is an idempotent no-op; flipping it is rejected);
    * ``same``: the two current groups may not contain two records of the same
      cohort, and may not be linked by an active ``different`` edge;
    * ``different``: the two records may not already be connected by a
      transitive ``same`` path, and may not already sit in one group.

    ``demo_pause_seconds`` parks the request inside the (lock-free)
    computation phase so the interleaving demo can drive a second entry
    point while this one is "thinking".
    """
    if decision not in ("same", "different"):
        raise ConflictError("decision 只能是 same 或 different",
                            {"code": "bad_decision", "decision": decision})
    if record_a == record_b:
        raise ConflictError("不能对同一条记录作出裁决",
                            {"code": "self_pair", "record_id": record_a})

    left, right = _pair(record_a, record_b)
    return _run_optimistic(
        lambda snap: _plan_adjudicate(snap, left, right, decision),
        pause_seconds=demo_pause_seconds, use_gate=True)


def revoke(adjudication_id: int, demo_pause_seconds: float = 0.0) -> dict:
    """Revoke an active adjudication and recompute groups from scratch.

    The partition is rebuilt purely from the remaining active ``same`` edges,
    so removing an edge may split a group into several connected components.
    ``different`` edges never join components, but are re-validated after the
    rebuild so a revoked decision can never leave the invariant broken.
    """
    return _run_optimistic(
        lambda snap: _plan_revoke(snap, adjudication_id),
        pause_seconds=demo_pause_seconds, use_gate=True)


# --------------------------------------------------------------------------- #
# CSV import / export
# --------------------------------------------------------------------------- #

# Header aliases: the import accepts both Chinese and English column names.
_HEADER_ALIASES = {
    "cohort": "cohort", "队列": "cohort",
    "source_id": "source_id", "sourceid": "source_id",
    "来源id": "source_id", "来源": "source_id",
    "alias": "alias", "化名": "alias", "name": "alias",
}


def import_csv(content: bytes | str) -> dict:
    """Import enrollment rows.

    Original rows are never modified: ``(cohort, source_id)`` is the natural
    key, and re-importing an identical row is skipped rather than updated.
    Each new record starts life in its own singleton group.  Parsing and
    de-duplication happen outside any lock; the commit only applies the
    inserts after checking that no concurrent import has landed meanwhile.
    """
    if isinstance(content, bytes):
        text = content.decode("utf-8-sig")
    else:
        text = content.lstrip("﻿")
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        raise ConflictError("CSV 内容为空或缺少表头", {"code": "empty_csv"})

    columns = {}
    for raw in reader.fieldnames:
        key = (raw or "").strip().lower()
        if key in _HEADER_ALIASES:
            columns[_HEADER_ALIASES[key]] = raw.strip()
    missing = [c for c in ("cohort", "source_id", "alias") if c not in columns]
    if missing:
        raise ConflictError(
            f"CSV 缺少必需列: {', '.join(missing)}",
            {"code": "missing_columns", "missing": missing,
             "found": reader.fieldnames},
        )

    rows: list[tuple[str, str, str]] = []
    seen_in_file: set[tuple[str, str]] = set()
    duplicate_in_file = 0
    line_errors = []
    for line_no, raw_row in enumerate(reader, start=2):
        try:
            cohort = (raw_row.get(columns["cohort"]) or "").strip()
            source_id = (raw_row.get(columns["source_id"]) or "").strip()
            alias = (raw_row.get(columns["alias"]) or "").strip()
        except AttributeError:
            line_errors.append({"line": line_no, "error": "行列格式错误"})
            continue
        if not cohort or not source_id or not alias:
            line_errors.append({"line": line_no, "error": "存在空字段"})
            continue
        key = (cohort, source_id)
        if key in seen_in_file:
            duplicate_in_file += 1
            continue
        seen_in_file.add(key)
        rows.append((cohort, source_id, alias))

    return _run_optimistic(
        lambda snap: _plan_import(snap, rows,
                                  duplicate_in_file=duplicate_in_file,
                                  line_errors=line_errors,
                                  total_rows_read=len(rows) + duplicate_in_file))


def export_mapping_csv() -> str:
    """Render the source-to-group mapping for one committed snapshot.

    The whole query runs through a single read-only connection/transaction, so
    every emitted row reflects the same committed state even if another
    process adjudicates while the download is in progress.
    """
    conn = get_read_conn()
    try:
        rows = conn.execute(
            """SELECT r.cohort, r.source_id, r.alias, rg.group_id
                 FROM records r JOIN record_groups rg ON rg.record_id = r.id
                ORDER BY rg.group_id, r.cohort, r.source_id"""
        ).fetchall()
    finally:
        conn.close()

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["cohort", "source_id", "alias", "group_id"])
    for row in rows:
        writer.writerow([row["cohort"], row["source_id"], row["alias"], row["group_id"]])
    return buf.getvalue()


# --------------------------------------------------------------------------- #
# Read models
# --------------------------------------------------------------------------- #

def list_records() -> list[dict]:
    conn = get_read_conn()
    try:
        rows = conn.execute(
            """SELECT r.id, r.cohort, r.source_id, r.alias, rg.group_id
                 FROM records r JOIN record_groups rg ON rg.record_id = r.id
                ORDER BY r.id"""
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def list_groups() -> list[dict]:
    conn = get_read_conn()
    try:
        groups = conn.execute(
            "SELECT group_id FROM groups_t ORDER BY group_id"
        ).fetchall()
        result = []
        for g in groups:
            members = conn.execute(
                """SELECT r.id, r.cohort, r.source_id, r.alias
                     FROM record_groups rg JOIN records r ON r.id = rg.record_id
                    WHERE rg.group_id = ? ORDER BY r.cohort, r.id""",
                (g["group_id"],),
            ).fetchall()
            result.append({"group_id": g["group_id"],
                           "members": [_record_brief(m) for m in members]})
        return result
    finally:
        conn.close()


def list_adjudications(active_only: bool = True) -> list[dict]:
    conn = get_read_conn()
    try:
        sql = """SELECT a.id, a.left_id, a.right_id, a.decision, a.active,
                        a.created_at, a.revoked_at,
                        l.cohort AS left_cohort, l.source_id AS left_source_id,
                        l.alias AS left_alias,
                        r.cohort AS right_cohort, r.source_id AS right_source_id,
                        r.alias AS right_alias
                   FROM adjudications a
                   JOIN records l ON l.id = a.left_id
                   JOIN records r ON r.id = a.right_id"""
        if active_only:
            sql += " WHERE a.active = 1"
        sql += " ORDER BY a.id"
        return [dict(r) for r in conn.execute(sql).fetchall()]
    finally:
        conn.close()


def _same_edges(conn: sqlite3.Connection) -> dict[int, set[int]]:
    """Build the undirected graph of active ``same`` adjudications."""
    graph: dict[int, set[int]] = {}
    for row in conn.execute(
        "SELECT left_id, right_id FROM adjudications WHERE decision = 'same' AND active = 1"
    ):
        graph.setdefault(row["left_id"], set()).add(row["right_id"])
        graph.setdefault(row["right_id"], set()).add(row["left_id"])
    return graph


def same_path(record_a: int, record_b: int) -> dict:
    """Return the shortest active same-person connection path between records."""
    left, right = _pair(record_a, record_b)
    conn = get_read_conn()
    try:
        _get_record(conn, left)
        _get_record(conn, right)
        graph = _same_edges(conn)
        path = _bfs_path(graph, left, right)
        if path is None:
            return {"connected": False, "path": []}
        return {"connected": True,
                "path": [_record_brief(_get_record(conn, rid)) for rid in path]}
    finally:
        conn.close()


def stats() -> dict:
    conn = get_read_conn()
    try:
        return {
            "records": conn.execute("SELECT COUNT(*) AS c FROM records").fetchone()["c"],
            "groups": conn.execute("SELECT COUNT(*) AS c FROM groups_t").fetchone()["c"],
            "active_adjudications": conn.execute(
                "SELECT COUNT(*) AS c FROM adjudications WHERE active = 1"
            ).fetchone()["c"],
            "same_edges": conn.execute(
                "SELECT COUNT(*) AS c FROM adjudications WHERE active = 1 AND decision = 'same'"
            ).fetchone()["c"],
            "different_edges": conn.execute(
                "SELECT COUNT(*) AS c FROM adjudications WHERE active = 1 AND decision = 'different'"
            ).fetchone()["c"],
        }
    finally:
        conn.close()
