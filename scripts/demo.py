#!/usr/bin/env python3
"""End-to-end demo against two web processes sharing one SQLite database.

Usage:
    python scripts/demo.py [--base1 http://127.0.0.1:8101]
                          [--base2 http://127.0.0.1:8102]
                          [--csv sample_data/sample_roster.csv]

The script imports the fictional roster through web1, then drives a full
adjudication / conflict / revoke / export scenario while alternating between
the two processes, proving the shared database behaves as one system.
Run it with an isolated interpreter (``python -I`` is fine).
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import sys
import urllib.error
import urllib.request


def call(method: str, base: str, path: str, payload: dict | None = None) -> tuple[int, dict | str]:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        base + path, data=data,
        headers={"Content-Type": "application/json"}, method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read().decode()
            try:
                return resp.status, json.loads(body)
            except json.JSONDecodeError:
                return resp.status, body
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def show(step: str, status: int, body) -> None:
    print(f"\n=== {step} -> HTTP {status}")
    if isinstance(body, (dict, list)):
        print(json.dumps(body, ensure_ascii=False, indent=2)[:1800])
    else:
        print(str(body)[:800])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base1", default="http://127.0.0.1:8101")
    ap.add_argument("--base2", default="http://127.0.0.1:8102")
    ap.add_argument("--csv", default="sample_data/sample_roster.csv")
    args = ap.parse_args()

    # 1. health of both replicas
    for base in (args.base1, args.base2):
        show(f"健康检查 {base}", *call("GET", base, "/healthz"))

    # 2. import via web1 (multipart upload, as the browser does)
    boundary = "----erdemoboundary"
    with open(args.csv, "rb") as fh:
        content = fh.read()
    body = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="file"; '
        'filename="sample_roster.csv"\r\n'
        "Content-Type: text/csv\r\n\r\n"
    ).encode() + content + f"\r\n--{boundary}--\r\n".encode()
    req = urllib.request.Request(
        args.base1 + "/api/import/file", data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        show("web1 导入样例 CSV", resp.status, json.loads(resp.read()))

    # re-import is append-only: identical rows are skipped
    req.data = body
    with urllib.request.urlopen(req, timeout=15) as resp:
        show("web1 重复导入（应全部跳过，原记录不变）",
             resp.status, json.loads(resp.read()))

    # 3. adjudications alternating across both processes
    show("web2: 1 与 5 判同人", *call("POST", args.base2, "/api/adjudicate",
         {"record_a": 1, "record_b": 5, "decision": "same"}))
    show("web1: 5 与 13 判同人（传递合并为 3 队列组）",
         *call("POST", args.base1, "/api/adjudicate",
               {"record_a": 5, "record_b": 13, "decision": "same"}))
    show("web2: 1 与 9 判同人（合法，加入第 4 个队列）",
         *call("POST", args.base2, "/api/adjudicate",
               {"record_a": 1, "record_b": 9, "decision": "same"}))
    show("web1: 9 与 5 判不同人（应 409：已被同人路径连通，附路径）",
         *call("POST", args.base1, "/api/adjudicate",
               {"record_a": 9, "record_b": 5, "decision": "different"}))

    # 4. different barrier then blocked merge across the barrier.
    #    2(CARDIO-A)~8(CARDIO-B) same builds a two-cohort group;
    #    8 vs 14(REHAB-D) different marks a barrier; merging 2 into 14 would
    #    pull the two different-person records into one group.
    show("web1: 2 与 8 判同人（跨队列合并）",
         *call("POST", args.base1, "/api/adjudicate",
               {"record_a": 2, "record_b": 8, "decision": "same"}))
    show("web2: 8 与 14 判不同人（互斥屏障）",
         *call("POST", args.base2, "/api/adjudicate",
               {"record_a": 8, "record_b": 14, "decision": "different"}))
    show("web1: 尝试合并 2 与 14（应 409：互斥的 8/14 将落入同组，附连接路径）",
         *call("POST", args.base1, "/api/adjudicate",
               {"record_a": 2, "record_b": 14, "decision": "same"}))

    # 5. revoke and re-partition (issued against web2, visible on web1)
    adj = call("GET", args.base2, "/api/adjudications")[1]
    first_edge = next(a for a in adj if a["left_id"] == 1 and a["right_id"] == 5)
    show(f"web2: 撤销裁决 #{first_edge['id']}（1~5）并重新拆组",
         *call("POST", args.base2, "/api/revoke",
               {"adjudication_id": first_edge["id"]}))
    show("web1: 分组视图（应已重新拆分）", *call("GET", args.base1, "/api/groups"))

    # 6. snapshot export from each process
    for base in (args.base1, args.base2):
        status, text = call("GET", base, "/api/export")
        rows = list(csv.DictReader(io.StringIO(text)))
        print(f"\n=== {base} 导出快照 -> HTTP {status}, {len(rows)} 行 ===")
        print("\n".join(text.splitlines()[:6]))

    # 7. final stats
    show("最终统计", *call("GET", args.base1, "/api/stats"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
