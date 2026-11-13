"""Domain logic: import, adjudication, revocation, grouping, export.

Concurrency model
-----------------
Every mutation runs inside :func:`app.db.write_tx` (``BEGIN IMMEDIATE``), so
concurrent adjudications are serialised by SQLite's writer lock.  All graph
traversals and partition checks run *inside* that same transaction and
therefore see a single consistent snapshot; a request that violates any
constraint raises :class:`ConflictError` and the whole transaction is rolled
back, leaving the previously committed state untouched.

Read paths (group listing, export, path lookup) use read-only connections and
never acquire a write lock.
"""

from __future__ import annotations

import csv
import io
import sqlite3
from collections import deque
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


def _active_decision(conn: sqlite3.Connection, left: int, right: int) -> Optional[str]:
    row = conn.execute(
        "SELECT decision FROM adjudications WHERE left_id = ? AND right_id = ? AND active = 1",
        (left, right),
    ).fetchone()
    return row["decision"] if row else None


def _group_of(conn: sqlite3.Connection, record_id: int) -> int:
    row = conn.execute(
        "SELECT group_id FROM record_groups WHERE record_id = ?", (record_id,)
    ).fetchone()
    if row is None:  # pragma: no cover - every imported record gets a group
        raise ConflictError("记录未分配分组，数据不一致",
                           {"code": "inconsistent_state", "record_id": record_id})
    return row["group_id"]


def _group_members(conn: sqlite3.Connection, group_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        """SELECT r.id, r.cohort, r.source_id, r.alias
             FROM record_groups rg JOIN records r ON r.id = rg.record_id
            WHERE rg.group_id = ? ORDER BY r.id""",
        (group_id,),
    ).fetchall()


def _same_edges(conn: sqlite3.Connection) -> dict[int, set[int]]:
    """Build the undirected graph of active ``same`` adjudications."""
    graph: dict[int, set[int]] = {}
    for row in conn.execute(
        "SELECT left_id, right_id FROM adjudications WHERE decision = 'same' AND active = 1"
    ):
        graph.setdefault(row["left_id"], set()).add(row["right_id"])
        graph.setdefault(row["right_id"], set()).add(row["left_id"])
    return graph


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


def _record_brief(row: sqlite3.Row) -> dict:
    return {"id": row["id"], "cohort": row["cohort"],
            "source_id": row["source_id"], "alias": row["alias"]}


def _active_cross_group_different(
    conn: sqlite3.Connection, group_a: int, group_b: int
) -> list[sqlite3.Row]:
    """Active ``different`` edges with one endpoint in each of two groups."""
    return conn.execute(
        """SELECT a.left_id, a.right_id
             FROM adjudications a
             JOIN record_groups rg1 ON rg1.record_id = a.left_id
             JOIN record_groups rg2 ON rg2.record_id = a.right_id
            WHERE a.active = 1 AND a.decision = 'different'
              AND rg1.group_id = ? AND rg2.group_id = ?""",
        (group_a, group_b),
    ).fetchall()


# --------------------------------------------------------------------------- #
# Adjudication
# --------------------------------------------------------------------------- #

def adjudicate(record_a: int, record_b: int, decision: str) -> dict:
    """Create an active adjudication between two records.

    Constraints (validated in one write transaction against the committed
    snapshot; failure rolls everything back):

    * the two records must exist and be distinct;
    * a pair may only hold one active adjudication (re-submitting the same
      decision is an idempotent no-op; flipping it is rejected);
    * ``same``: the two current groups may not contain two records of the same
      cohort, and may not be linked by an active ``different`` edge;
    * ``different``: the two records may not already be connected by a
      transitive ``same`` path, and may not already sit in one group.
    """
    if decision not in ("same", "different"):
        raise ConflictError("decision 只能是 same 或 different",
                           {"code": "bad_decision", "decision": decision})
    if record_a == record_b:
        raise ConflictError("不能对同一条记录作出裁决",
                           {"code": "self_pair", "record_id": record_a})

    left, right = _pair(record_a, record_b)

    with write_tx() as conn:
        rec_left = _get_record(conn, left)
        rec_right = _get_record(conn, right)

        existing = _active_decision(conn, left, right)
        if existing is not None:
            if existing == decision:
                return {"status": "noop", "message": "该裁决已存在，状态未改变",
                        "left_id": left, "right_id": right, "decision": decision}
            raise ConflictError(
                f"该记录对已有生效裁决（{existing}），不能改为 {decision}；请先撤销原裁决",
                {"code": "pair_already_adjudicated", "left_id": left,
                 "right_id": right, "existing": existing},
            )

        if decision == "same":
            # Within one component the pair is only transitively linked (an
            # explicit edge would have been the no-op handled above), so this
            # records the explicit edge; otherwise the groups are validated and
            # merged.
            result = _adjudicate_same(conn, left, right, rec_left, rec_right)
        else:
            result = _adjudicate_different(conn, left, right, rec_left, rec_right)

        conn.execute(
            "INSERT INTO adjudications (left_id, right_id, decision, active, created_at)"
            " VALUES (?, ?, ?, 1, ?)",
            (left, right, decision, _now()),
        )
        return result


def _adjudicate_same(
    conn: sqlite3.Connection,
    left: int, right: int,
    rec_left: sqlite3.Row, rec_right: sqlite3.Row,
) -> dict:
    group_a = _group_of(conn, left)
    group_b = _group_of(conn, right)

    if group_a == group_b:
        # Already transitively linked: the edge simply records the decision.
        return {"status": "added_edge_within_group",
                "message": "两条记录已在同一分组，裁决作为显式同人边保存"}

    members_a = _group_members(conn, group_a)
    members_b = _group_members(conn, group_b)

    # Constraint 1: at most one record per cohort inside a merged group.
    cohorts_a = {}
    for m in members_a:
        cohorts_a.setdefault(m["cohort"], []).append(m)
    cohort_conflicts = []
    for m in members_b:
        if m["cohort"] in cohorts_a:
            cohort_conflicts.append({
                "cohort": m["cohort"],
                "records": [_record_brief(x) for x in cohorts_a[m["cohort"]]]
                           + [_record_brief(m)],
            })

    # Constraint 2: no active "different" edge between the two groups.
    diff_edges = _active_cross_group_different(conn, group_a, group_b)
    graph = _same_edges(conn)
    different_conflicts = []
    for edge in diff_edges:
        different_conflicts.append({
            "left": _record_brief(_get_record(conn, edge["left_id"])),
            "right": _record_brief(_get_record(conn, edge["right_id"])),
            # Show how each endpoint reaches the two records being merged.
            "path_to_a": _bfs_path(graph, edge["left_id"], left)
                         or _bfs_path(graph, edge["left_id"], right),
            "path_to_b": _bfs_path(graph, edge["right_id"], right)
                         or _bfs_path(graph, edge["right_id"], left),
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

    # Merge group B into group A by re-pointing its members.
    conn.execute("DELETE FROM record_groups WHERE group_id = ?", (group_b,))
    conn.executemany(
        "INSERT INTO record_groups (record_id, group_id) VALUES (?, ?)",
        [(m["id"], group_a) for m in members_b],
    )
    conn.execute("DELETE FROM groups_t WHERE group_id = ?", (group_b,))
    return {"status": "merged", "message": "同人裁决生效，两个分组合并",
            "merged_group_id": group_a}


def _adjudicate_different(
    conn: sqlite3.Connection,
    left: int, right: int,
    rec_left: sqlite3.Row, rec_right: sqlite3.Row,
) -> dict:
    group_a = _group_of(conn, left)
    group_b = _group_of(conn, right)

    if group_a == group_b:
        # Same group means a transitive "same" path exists (or the records
        # were already merged). Locate the path for the error message.
        graph = _same_edges(conn)
        path = _bfs_path(graph, left, right)
        path_records = [_record_brief(_get_record(conn, rid)) for rid in (path or [left, right])]
        raise ConflictError(
            "两条记录已通过同人关系连接，不能判为不同人",
            {"code": "already_same_path",
             "record_a": _record_brief(rec_left),
             "record_b": _record_brief(rec_right),
             "same_path": path_records},
        )

    # Different groups: the edge is sufficient; groups themselves are unchanged.
    return {"status": "different_edge",
            "message": "不同人裁决生效，两个分组保持独立并被标记互斥"}


# --------------------------------------------------------------------------- #
# Revocation and re-partitioning
# --------------------------------------------------------------------------- #

def revoke(adjudication_id: int) -> dict:
    """Revoke an active adjudication and recompute groups from scratch.

    The partition is rebuilt purely from the remaining active ``same`` edges,
    so removing an edge may split a group into several connected components.
    ``different`` edges never join components, but are re-validated after the
    rebuild so a revoked decision can never leave the invariant broken.
    """
    with write_tx() as conn:
        row = conn.execute(
            "SELECT id, left_id, right_id, decision, active FROM adjudications WHERE id = ?",
            (adjudication_id,),
        ).fetchone()
        if row is None:
            raise ConflictError(f"裁决 {adjudication_id} 不存在",
                               {"code": "adjudication_not_found",
                                "adjudication_id": adjudication_id})
        if not row["active"]:
            return {"status": "noop", "message": "该裁决已撤销，状态未改变",
                    "adjudication_id": adjudication_id}

        conn.execute(
            "UPDATE adjudications SET active = 0, revoked_at = ? WHERE id = ?",
            (_now(), adjudication_id),
        )
        before_groups = conn.execute(
            "SELECT COUNT(*) AS c FROM groups_t"
        ).fetchone()["c"]
        result = _rebuild_groups(conn)
        result.update({"status": "revoked",
                       "message": "裁决已撤销并重新计算分组",
                       "adjudication_id": adjudication_id,
                       "groups_before": before_groups,
                       "groups_after": result["group_count"]})
        return result


def _rebuild_groups(conn: sqlite3.Connection) -> dict:
    """Recompute the partition from active ``same`` edges, in-transaction."""
    graph = _same_edges(conn)
    all_records = conn.execute("SELECT id FROM records ORDER BY id").fetchall()

    component_of: dict[int, int] = {}
    components: list[list[int]] = []
    for rec in all_records:
        start = rec["id"]
        if start in component_of:
            continue
        comp: list[int] = []
        queue = deque([start])
        component_of[start] = len(components)
        while queue:
            node = queue.popleft()
            comp.append(node)
            for nxt in graph.get(node, ()):
                if nxt not in component_of:
                    component_of[nxt] = len(components)
                    queue.append(nxt)
        components.append(comp)

    # Reset the partition tables and write the new one.
    conn.execute("DELETE FROM record_groups")
    conn.execute("DELETE FROM groups_t")
    conn.executemany("INSERT INTO groups_t (group_id) VALUES (?)",
                     [(i + 1,) for i in range(len(components))])
    conn.executemany(
        "INSERT INTO record_groups (record_id, group_id) VALUES (?, ?)",
        [(rid, component_of[rid] + 1) for rid in sorted(component_of)],
    )

    # Re-validate: no active different edge may end inside one component.
    violations = []
    for edge in conn.execute(
        "SELECT left_id, right_id FROM adjudications WHERE active = 1 AND decision = 'different'"
    ):
        if component_of[edge["left_id"]] == component_of[edge["right_id"]]:
            violations.append((edge["left_id"], edge["right_id"]))
    if violations:
        # Should be impossible given adjudication validation; guard anyway so
        # that a broken invariant is never silently committed.
        raise ConflictError("撤销后重新分组发现不同人记录落入同组，操作已回滚",
                           {"code": "invariant_violation_after_rebuild",
                            "violating_pairs": violations})

    return {"group_count": len(components),
            "component_sizes": [len(c) for c in components]}


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
    Each new record starts life in its own singleton group.
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

    inserted = skipped = 0
    with write_tx() as conn:
        for cohort, source_id, alias in rows:
            existing = conn.execute(
                "SELECT 1 FROM records WHERE cohort = ? AND source_id = ?",
                (cohort, source_id),
            ).fetchone()
            if existing:
                skipped += 1
                continue
            cur = conn.execute(
                "INSERT INTO records (cohort, source_id, alias) VALUES (?, ?, ?)",
                (cohort, source_id, alias),
            )
            new_id = cur.lastrowid
            group_cur = conn.execute("INSERT INTO groups_t DEFAULT VALUES")
            group_id = group_cur.lastrowid
            conn.execute(
                "INSERT INTO record_groups (record_id, group_id) VALUES (?, ?)",
                (new_id, group_id),
            )
            inserted += 1

    return {"inserted": inserted, "skipped_existing": skipped,
            "duplicate_in_file": duplicate_in_file,
            "line_errors": line_errors, "total_rows_read": len(rows) + duplicate_in_file}


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
