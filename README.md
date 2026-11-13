# 随访入组名册 · 关联裁决服务（FastAPI + SQLite）

随访数据工程师合并不同队列的入组名册前，需要人工核定哪些记录属于同一参与者。
本服务提供一个浏览器工作台与 REST API：导入含 **队列 / 来源ID / 化名** 的 CSV，
以「队列 + 来源ID」共同标识记录，**原始记录永不修改**；数据工程师选择两条记录
确认**同一参与者**或**不同人**，也可撤销裁决。系统维护一份满足完整分组约束的
参与者分组，并支持下载「来源 → 分组」映射 CSV。

> 仓库中的人物、ID 均为虚构样例（见 `sample_data/`），不包含任何真实患者信息。

## 一、领域规则与不变量

1. 记录以 `(cohort, source_id)` 为自然键；重复导入按跳过处理，原行不更新。
2. `same`（同一参与者）关系具有传递性：A~B、B~C ⇒ A、B、C 同组。
3. 每个分组内，**同一队列最多一条记录**。
4. 判为 `different`（不同人）的两条记录**永远不得落入同一分组**。
5. 任何违反 3、4 的裁决请求**整次拒绝（HTTP 409）**，响应中给出冲突记录与
   同人连接路径；数据库回滚到提交前状态，失败保留原状。
6. 撤销裁决后，分组按剩余的生效 `same` 边重新计算（可能拆成多个连通分量），
   裁决与分组在同一事务内一起持久生效。
7. 导出 CSV 在单个只读事务/快照内完成，所有行反映同一个已提交状态。

## 二、并发模型（双 Web 进程共库）

- 部署两个（可更多）独立 uvicorn 进程，通过共享卷访问同一个 SQLite 数据库文件。
- SQLite 启用 **WAL**：读写不互相阻塞；所有关系计算（图遍历、分组列表、导出）
  使用 `mode=ro` + `query_only=ON` 的**只读连接，从不获取写锁**
  （见 `app/db.py:get_read_conn`）。
- 每个写操作在 `BEGIN IMMEDIATE` 事务中执行：先拿到预留锁，再在同一事务内基于
  一致快照做全部约束校验，最后提交。并发的不同记录对裁决因此被串行化——
  「分别合法、合在一起却冲突」的两个请求，只有先提交者生效，后者在新快照上
  校验失败并整体回滚。`busy_timeout` 让后来者等待而不是立刻报错。
- 生效裁决对的唯一性由部分唯一索引
  `UNIQUE(left_id, right_id) WHERE active = 1` 在数据库层兜底。

## 三、快速开始（Docker Compose，推荐）

```bash
docker compose up --build
# web1: http://localhost:8001   web2: http://localhost:8002
# seed 服务会自动把 sample_data/sample_roster.csv 导入共享数据库后退出
```

- 两个 Web 进程共用命名卷 `er-data` 中的 `/data/er.db`。
- 重置演示数据：`docker compose down -v && docker compose up --build`。
- 浏览器打开任一地址即可使用；`/docs` 为 Swagger UI，`/healthz` 为健康检查。

## 四、本地直接运行（无 Docker）

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
export ER_DB_PATH=$(pwd)/data/er.db      # 可选，默认即此路径

# 进程 1 / 进程 2，共享同一个数据库文件
uvicorn app.main:app --host 127.0.0.1 --port 8101 &
uvicorn app.main:app --host 127.0.0.1 --port 8102 &

# 导入虚构样例（任选一个进程）
curl -X POST http://127.0.0.1:8101/api/import/file \
  -F "file=@sample_data/sample_roster.csv;type=text/csv"

# 端到端演示（双进程交替调用，含冲突与撤销）
python -I scripts/demo.py

# 跨进程并发裁决检查（动态构造合法/冲突竞赛，校验最终不变量）
python -I scripts/concurrency_check.py

# 自动化测试
pytest -q
```

## 五、REST API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/import` | 粘贴 CSV 文本导入（JSON：`{"raw": "..."}`） |
| POST | `/api/import/file` | multipart 文件上传导入 |
| GET | `/api/records` | 记录列表（含当前 `group_id`） |
| GET | `/api/groups` | 当前分组视图 |
| GET | `/api/adjudications?active_only=true` | 裁决列表（`false` 含已撤销历史） |
| POST | `/api/adjudicate` | 提交裁决 `{record_a, record_b, decision: same\|different}` |
| POST | `/api/revoke` | 撤销 `{adjudication_id}` 并重新拆组 |
| POST | `/api/same-path` | 查询两记录的最短同人连接路径 |
| GET | `/api/export` | 下载同一快照的 `cohort,source_id,alias,group_id` 映射 CSV |
| GET | `/api/stats` | 计数概览 |
| GET | `/healthz` | 健康检查 |

### 冲突响应示例（409，整次拒绝）

```json
{
  "detail": "合并将违反分组约束，整次裁决已拒绝",
  "code": "merge_conflict",
  "cohort_conflicts": [
    {"cohort": "CARDIO-A", "records": [ {"id": 1, "...": "..."}, {"id": 2, "...": "..."} ]}
  ],
  "different_conflicts": [
    {"left": {"id": 7, "...": "..."}, "right": {"id": 14, "...": "..."},
     "path_to_a": [8, 2], "path_to_b": [14]}
  ]
}
```

## 六、数据模型

```text
records(id, cohort, source_id, alias, imported_at)   UNIQUE(cohort, source_id)   -- 只追加，不更新
groups_t(group_id, created_at)
record_groups(record_id PK -> records, group_id -> groups_t)                     -- 当前分区
adjudications(id, left_id, right_id, decision[same|different],
              active, created_at, revoked_at)
    UNIQUE(left_id, right_id) WHERE active = 1                                   -- 部分唯一索引
```

- 裁决历史只追加：撤销是把 `active` 置 0 并记录 `revoked_at`，不删除历史行。
- 分组表是当前生效分区的物化结果，每次裁决合并或撤销重算都在同一写事务内更新。

## 七、目录结构

```text
app/
  db.py            # 连接管理：只读连接 + BEGIN IMMEDIATE 写事务、WAL/schema
  services.py      # 领域逻辑：导入、裁决、撤销重算、分组、路径、导出
  main.py          # FastAPI 路由、409 异常处理、生命周期
  static/index.html# 浏览器单页工作台（原生 JS，无构建步骤）
sample_data/       # 虚构样例 CSV 与演练说明
scripts/demo.py             # 双进程端到端演示
scripts/concurrency_check.py# 跨 OS 进程并发竞赛与不变量校验
tests/test_api.py  # 12 个 pytest 用例（约束/撤销/快照/并发/回滚）
docker-compose.yml # web1 + web2 共库 + 一次性 seed 导入
Dockerfile
```
