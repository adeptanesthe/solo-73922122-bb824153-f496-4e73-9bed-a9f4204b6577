#!/usr/bin/env python3
"""可控暂停关系计算的双入口交错演示。

针对两个共用同一 SQLite 数据库的 Web 进程（web1 / web2）：

1. 关闭 web1 的「关系计算闸门」（POST /api/demo/gate），web1 上的裁决会停在
   计算阶段——此时它不持有任何数据库锁；
2. 从 web2 提交**无关裁决**与**名册导入**：立即提交成功（打印耗时为证），
   不再像修复前那样等待 web1 的写锁；
3. 打开闸门后，web1 检测到计算期间状态已变化，自动在新快照上重算并提交——
   交错提交的裁决/导入既不被旧计算结果覆盖，也不丢失；
4. 再构造一对「分别合法、合在一起冲突」的交错裁决：恰好一个成功、
   另一个整次拒绝（HTTP 409），分组约束始终成立；
5. 最后从两个入口分别校验全部不变量。

用法：
    python -I scripts/interleaved_demo.py \
        [--base1 http://127.0.0.1:8001] [--base2 http://127.0.0.1:8002] \
        [--csv sample_data/sample_roster.csv]

默认地址对应 docker compose 暴露的端口；本地 uvicorn 起在 8101/8102 时用
--base1 http://127.0.0.1:8101 --base2 http://127.0.0.1:8102。
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import sys
import threading
import time
import urllib.error
import urllib.request


def call(method: str, base: str, path: str, payload: dict | None = None,
         timeout: float = 60.0) -> tuple[int, dict | str]:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        base + path, data=data,
        headers={"Content-Type": "application/json"}, method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode()
            try:
                return resp.status, json.loads(body)
            except json.JSONDecodeError:
                return resp.status, body
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def get(base, path):
    status, body = call("GET", base, path)
    assert status == 200, (base, path, status, body)
    return body


def post(base, path, payload):
    return call("POST", base, path, payload)


def show(step: str, status: int, body) -> None:
    print(f"\n=== {step} -> HTTP {status}")
    if isinstance(body, (dict, list)):
        print(json.dumps(body, ensure_ascii=False, indent=2)[:1200])
    else:
        print(str(body)[:600])


def gate(base: str, open_: bool) -> None:
    status, body = post(base, "/api/demo/gate", {"open": open_})
    assert status == 200 and body["open"] == open_, (status, body)
    print(f"[闸门] {base} 关系计算闸门 {'打开' if open_ else '关闭（本进程计算暂停）'}")


def find_disjoint_legal_pairs(groups: list[dict], count: int) -> list[tuple[int, int]]:
    """Up to ``count`` record pairs, each merging two groups with disjoint
    cohorts, and no record reused across pairs."""
    pairs: list[tuple[int, int]] = []
    used: set[int] = set()
    for i in range(len(groups)):
        for j in range(i + 1, len(groups)):
            if len(pairs) >= count:
                return pairs
            ga, gb = groups[i], groups[j]
            cohorts_a = {m["cohort"] for m in ga["members"]}
            cohorts_b = {m["cohort"] for m in gb["members"]}
            if not cohorts_a.isdisjoint(cohorts_b):
                continue
            a, b = ga["members"][0]["id"], gb["members"][0]["id"]
            if a in used or b in used:
                continue
            pairs.append((a, b))
            used.update((a, b))
    return pairs


def verify_invariants(bases: list[str]) -> list[str]:
    """Check every partition invariant from each entry point's perspective."""
    failures = []
    for base in bases:
        groups = get(base, "/api/groups")
        records = get(base, "/api/records")
        active = get(base, "/api/adjudications?active_only=true")
        grouped = {m["id"] for g in groups for m in g["members"]}
        if grouped != {r["id"] for r in records}:
            failures.append(f"{base} 存在未分组或重复分组的记录")
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
    return failures


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base1", default="http://127.0.0.1:8001")
    ap.add_argument("--base2", default="http://127.0.0.1:8002")
    ap.add_argument("--csv", default="sample_data/sample_roster.csv")
    args = ap.parse_args()
    web1, web2 = args.base1, args.base2

    for base in (web1, web2):
        show(f"健康检查 {base}", *call("GET", base, "/healthz"))

    # Ensure there is some data to adjudicate (idempotent append-only import).
    if get(web1, "/api/stats")["records"] == 0:
        with open(args.csv, "rb") as fh:
            content = fh.read()
        boundary = "----erinterleaved"
        body = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\";"
                f" filename=\"roster.csv\"\r\nContent-Type: text/csv\r\n\r\n"
                ).encode() + content + f"\r\n--{boundary}--\r\n".encode()
        req = urllib.request.Request(
            web1 + "/api/import/file", data=body,
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            show("web1 导入样例 CSV", resp.status, json.loads(resp.read()))

    # ------------------------------------------------------------------ #
    # 阶段 1：web1 计算被暂停期间，web2 的无关裁决与导入立即提交
    # ------------------------------------------------------------------ #
    print("\n" + "=" * 70)
    print("阶段 1：暂停 web1 的关系计算，web2 的无关裁决/导入无需等待")
    print("=" * 70)
    pairs = find_disjoint_legal_pairs(get(web1, "/api/groups"), 2)
    if len(pairs) < 2:
        print("当前状态找不到两对互不相关的可合并记录，跳过阶段 1")
    else:
        (a1, b1), (a2, b2) = pairs
        gate(web1, False)
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                slow = pool.submit(post, web1, "/api/adjudicate",
                                   {"record_a": a1, "record_b": b1,
                                    "decision": "same"})
                time.sleep(0.5)  # 确认 web1 的请求已进入计算阶段
                print(f"[web1] 裁决 {a1}~{b1} same 已提交，正停在（不持锁的）计算阶段…")

                start = time.monotonic()
                s2, r2 = post(web2, "/api/adjudicate",
                              {"record_a": a2, "record_b": b2, "decision": "same"})
                elapsed = time.monotonic() - start
                print(f"[web2] 无关裁决 {a2}~{b2} same -> HTTP {s2}，"
                      f"耗时 {elapsed:.2f}s（未等待 web1 的计算）")
                assert s2 == 200 and elapsed < 5, (s2, elapsed, r2)

                start = time.monotonic()
                tag = int(time.time())
                s3, r3 = post(web2, "/api/import", {"raw":
                    "cohort,source_id,alias\n"
                    f"DEMO-X,X-{tag}-1,演示新增一\nDEMO-X,X-{tag}-2,演示新增二"})
                elapsed = time.monotonic() - start
                print(f"[web2] 名册导入 2 条新记录 -> HTTP {s3}，"
                      f"耗时 {elapsed:.2f}s（计算期间导入不被阻塞）")
                assert s3 == 200 and r3["inserted"] == 2, (s3, r3)

                gate(web1, True)
                s1, r1 = slow.result(timeout=60)
                print(f"[web1] 闸门打开后完成 -> HTTP {s1}：{r1.get('message')}"
                      f"（检测到并发提交，自动在新快照上重算）")
                assert s1 == 200, (s1, r1)
        finally:
            gate(web1, True)  # 确保闸门复位

        active = get(web1, "/api/adjudications?active_only=true")
        have = {(a["left_id"], a["right_id"]) for a in active}
        pair1, pair2 = tuple(sorted((a1, b1))), tuple(sorted((a2, b2)))
        assert pair1 in have and pair2 in have, (pair1, pair2, have)
        print(f"[校验] 两个交错的裁决都已生效：{pair1} 与 {pair2}，无覆盖、无丢失")

    # ------------------------------------------------------------------ #
    # 阶段 2：分别合法、共同冲突的交错裁决 —— 恰有一个被整次拒绝
    # ------------------------------------------------------------------ #
    print("\n" + "=" * 70)
    print("阶段 2：交错提交「分别合法、共同冲突」的合并，恰好一个成功")
    print("=" * 70)
    groups = get(web1, "/api/groups")
    singletons = [g for g in groups if len(g["members"]) == 1]
    trio = None
    for i in range(len(singletons)):
        for j in range(i + 1, len(singletons)):
            for k in range(j + 1, len(singletons)):
                ms = [singletons[i]["members"][0], singletons[j]["members"][0],
                      singletons[k]["members"][0]]
                if len({m["cohort"] for m in ms}) == 3:
                    trio = [m["id"] for m in ms]
                    break
            if trio:
                break
        if trio:
            break
    if trio is None:
        print("找不到三条队列互不相同的单例记录，跳过阶段 2")
    else:
        x, y, z = trio
        s, r = post(web2, "/api/adjudicate",
                    {"record_a": x, "record_b": y, "decision": "different"})
        assert s == 200, (s, r)
        print(f"[web2] 互斥边 {x}~{y} different -> 200（屏障）")

        gate(web1, False)
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                slow = pool.submit(post, web1, "/api/adjudicate",
                                   {"record_a": z, "record_b": x, "decision": "same"})
                time.sleep(0.5)
                print(f"[web1] 合并 {z}~{x} 停在计算阶段（单独看合法）…")
                s2, r2 = post(web2, "/api/adjudicate",
                              {"record_a": z, "record_b": y, "decision": "same"})
                print(f"[web2] 合并 {z}~{y} -> HTTP {s2}（单独看也合法，先提交生效）")
                assert s2 == 200, (s2, r2)
                gate(web1, True)
                s1, r1 = slow.result(timeout=60)
                print(f"[web1] 重算后 -> HTTP {s1}：{r1.get('detail') or r1.get('message')}")
                assert s1 == 409 and r1["code"] == "merge_conflict", (s1, r1)
                dc = r1["different_conflicts"][0]
                print(f"      冲突详情：不同人对 {dc['left']['id']}⚔{dc['right']['id']} "
                      f"将被并入同组，整次拒绝")
        finally:
            gate(web1, True)
        gm = {r_["id"]: r_["group_id"] for r_ in get(web1, "/api/records")}
        assert gm[x] != gm[y], "互斥记录落入同组！"
        print(f"[校验] 屏障 {x}⚔{y} 仍然成立，分组约束未被并发破坏")

    # ------------------------------------------------------------------ #
    # 阶段 3：两个入口分别校验全部不变量与计数一致性
    # ------------------------------------------------------------------ #
    print("\n" + "=" * 70)
    print("阶段 3：双入口不变量校验")
    print("=" * 70)
    failures = verify_invariants([web1, web2])
    stats1, stats2 = get(web1, "/api/stats"), get(web2, "/api/stats")
    print(f"web1 统计: {stats1}")
    print(f"web2 统计: {stats2}")
    if stats1 != stats2:
        failures.append(f"两个入口统计不一致: {stats1} != {stats2}")
    if failures:
        print("\n[FAIL] 不变量被破坏:")
        for f in failures:
            print(" -", f)
        return 1
    print("\n[OK] 计算期间另一入口可自由提交；交错变更无覆盖、无丢失；"
          "全部分组约束在两个入口视角下均成立。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
