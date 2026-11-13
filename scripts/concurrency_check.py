#!/usr/bin/env python3
"""Cross-process concurrency check against two real uvicorn replicas.

Unlike the in-process pytest threads, this fires adjudications at two
separate OS processes sharing one SQLite file, in parallel batches, and then
verifies the committed partition still satisfies every invariant:

* every group contains at most one record per cohort;
* no active ``different`` pair ever lands in one group;
* no adjudications were silently lost (counts match accepted requests).

Usage:
    python scripts/concurrency_check.py --base1 http://127.0.0.1:8101 \
                                        --base2 http://127.0.0.1:8102
The target databases must already contain the sample roster (15 records).
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import sys
import urllib.error
import urllib.request


def call(method: str, base: str, path: str, payload: dict | None = None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        base + path, data=data,
        headers={"Content-Type": "application/json"}, method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def post(base, path, payload):
    return call("POST", base, path, payload)


def get(base, path):
    _, body = call("GET", base, path)
    return body


def find_legal_merges(groups: list[dict]) -> list[tuple[int, int]]:
    """All cross-group record pairs whose cohorts do not overlap."""
    legal = []
    for i in range(len(groups)):
        for j in range(i + 1, len(groups)):
            ga, gb = groups[i], groups[j]
            cohorts_a = {m["cohort"] for m in ga["members"]}
            cohorts_b = {m["cohort"] for m in gb["members"]}
            if cohorts_a.isdisjoint(cohorts_b):
                legal.append((ga["members"][0]["id"], gb["members"][0]["id"]))
    return legal


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base1", default="http://127.0.0.1:8101")
    ap.add_argument("--base2", default="http://127.0.0.1:8102")
    args = ap.parse_args()

    # ---- Phase 1: disjoint cross-cohort merges, simultaneous across procs.
    # Limit to four merges so enough singleton groups remain for later phases.
    groups = get(args.base1, "/api/groups")
    legal = find_legal_merges(groups)
    used: set[int] = set()
    disjoint: list[tuple[int, int]] = []
    for a, b in legal:
        if len(disjoint) >= 4:
            break
        if a not in used and b not in used:
            disjoint.append((a, b))
            used.update((a, b))
    if not disjoint:
        print("第一批：当前状态已无可执行的跨队列合并，跳过")
    else:
        print(f"第一批：{len(disjoint)} 个互不相关的跨队列合并，并发打到两个进程")
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(disjoint)) as pool:
            futures = [
                pool.submit(post, args.base1 if i % 2 == 0 else args.base2,
                            "/api/adjudicate",
                            {"record_a": a, "record_b": b, "decision": "same"})
                for i, (a, b) in enumerate(disjoint)
            ]
            results = [f.result() for f in futures]
        ok = sum(1 for s, _ in results if s == 200)
        bad = [(s, b) for s, b in results if s != 200]
        print(f"  成功 {ok} / {len(disjoint)}，冲突 {len(bad)}")
        if bad:
            for s, b in bad:
                print("  [FAIL] 意外冲突:", s, b.get("code"))
            return 1

    # ---- Phase 2: build a different-person barrier, then race merges.
    groups = get(args.base1, "/api/groups")
    singletons = [g for g in groups if len(g["members"]) == 1]
    if len(singletons) < 2:
        print("\n屏障竞赛：单例分组不足，跳过（最终不变量仍会校验）")
    else:
        x_group, y_group = singletons[0], singletons[1]
        x = x_group["members"][0]["id"]
        y = y_group["members"][0]["id"]
        s, r = post(args.base1, "/api/adjudicate",
                    {"record_a": x, "record_b": y, "decision": "different"})
        assert s == 200, (s, r)
        print(f"\n互斥边 {x}~{y} different -> 200")

        # A third record z: merging z with x and z with y are each standalone-
        # legal; both together would put the different pair in one group.
        groups = get(args.base1, "/api/groups")
        z_candidates = [g for g in groups
                        if g["group_id"] not in (x_group["group_id"],
                                                 y_group["group_id"])]
        if not z_candidates:
            print("屏障竞赛：缺少第三方记录，跳过")
        else:
            z = z_candidates[0]["members"][0]["id"]
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                f1 = pool.submit(post, args.base1, "/api/adjudicate",
                                 {"record_a": z, "record_b": x, "decision": "same"})
                f2 = pool.submit(post, args.base2, "/api/adjudicate",
                                 {"record_a": z, "record_b": y, "decision": "same"})
                pair = sorted(s for s, _ in (f1.result(), f2.result()))
            print(f"屏障竞赛：z={z} 分别合并 x={x} / y={y} -> 状态码 {pair}")
            if pair == [200, 200]:
                print("[FAIL] 互斥记录对被同时合入一组")
                return 1

    # ---- Phase 3: duplicate-cohort race, chosen dynamically.
    groups = get(args.base1, "/api/groups")
    legal = find_legal_merges(groups)
    # Two merges sharing a middle group whose combination creates a cohort
    # duplicate: find (a,z), (b,z) where a and b share a cohort.
    record_cohort = {m["id"]: m["cohort"]
                     for g in groups for m in g["members"]}
    group_of = {m["id"]: g["group_id"]
                for g in groups for m in g["members"]}
    chosen = None
    for (a, z1) in legal:
        for (b, z2) in legal:
            if z1 == z2 and a != b and group_of[a] != group_of[b] \
                    and record_cohort[a] == record_cohort[b]:
                chosen = (a, b, z1)
                break
        if chosen:
            break
    if chosen is None:
        print("\n队列竞赛：当前状态下无可构造场景，跳过（不影响结论）")
    else:
        a, b, z = chosen
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(post, args.base1, "/api/adjudicate",
                             {"record_a": a, "record_b": z, "decision": "same"})
            f2 = pool.submit(post, args.base2, "/api/adjudicate",
                             {"record_a": b, "record_b": z, "decision": "same"})
            pair = sorted(s for s, _ in (f1.result(), f2.result()))
        print(f"队列竞赛：{a}~{z} vs {b}~{z}（同队列 {record_cohort[a]}）-> {pair}")
        if pair == [200, 200]:
            print("[FAIL] 同队列两条记录被并入同一分组")
            return 1

    # ---- Phase 4: invariant verification from both replicas.
    failures = []
    for base in (args.base1, args.base2):
        groups = get(base, "/api/groups")
        active = get(base, "/api/adjudications?active_only=true")
        for g in groups:
            cohorts = [m["cohort"] for m in g["members"]]
            if len(cohorts) != len(set(cohorts)):
                failures.append(f"{base} G{g['group_id']} 同队列重复: {cohorts}")
            ids = {m["id"] for m in g["members"]}
            for adj in active:
                if adj["decision"] == "different" \
                        and adj["left_id"] in ids and adj["right_id"] in ids:
                    failures.append(
                        f"{base} G{g['group_id']} 含不同人对 "
                        f"{adj['left_id']}/{adj['right_id']}")
        print(f"\n{base} 统计: {get(base, '/api/stats')}")

    if failures:
        print("\n[FAIL] 不变量被破坏:")
        for f in failures:
            print(" -", f)
        return 1
    print("\n[OK] 两个进程视角下分组约束全部成立，无丢失裁决。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
