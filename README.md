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

## 二、并发模型（双 Web 进程共库 · 关系计算不持写锁）

- 部署两个（可更多）独立 uvicorn 进程，通过共享卷访问同一个 SQLite 数据库文件。
- SQLite 启用 **WAL**：读写不互相阻塞；所有只读路径（分组列表、导出、路径查询）
  使用 `mode=ro` + `query_only=ON` 的只读连接，从不获取写锁
  （见 `app/db.py:get_read_conn`）。
- **关键设计：关系计算全程不持写锁。** 每个变更（裁决 / 撤销 / 导入）分三段执行
  （见 `app/services.py`）：
  1. **快照**：只读连接在单个读事务内读取一致快照（记录、裁决历史、当前分组、
     版本计数器），随即关闭——不持有任何锁；
  2. **计算**：图遍历、分组重算、冲突路径计算全部在内存中针对快照执行，
     **此阶段另一进程可照常提交无关裁决**（演示见「交错演示」一节）；
  3. **提交**：短暂的 `BEGIN IMMEDIATE` 写事务先复核 `meta` 表的版本计数器
     （`records_version` / `edges_version`）。若计算期间有其他裁决、撤销或导入
     提交，版本必然变化 → 本次写入回滚并**在新快照上自动重算**——交错提交的
     变更既不会被旧计算结果覆盖，也不会丢失；版本一致才把
     「裁决行 + 分组变更 + 版本递增」一次性写入：裁决与分组在同一事务内持久生效，
     任何失败都回滚到提交前状态。
- 校验基于快照、提交要求快照仍然有效，因此「分别合法、合在一起却冲突」的并发
  裁决只有先提交者生效；后者在新快照上重算时校验失败并整体回滚（HTTP 409）。
- `busy_timeout`（默认 10000ms，`ER_BUSY_TIMEOUT_MS` 可调）让并发写者等待而不是
  立刻报 `SQLITE_BUSY`；生效裁决对的唯一性由部分唯一索引
  `UNIQUE(left_id, right_id) WHERE active = 1` 在数据库层兜底。
- 极端持续写冲突下乐观重试可能饥饿：重试 8 次仍未提交时，最后一次尝试退化为在
  写事务内完成校验（修复前行为），保证请求有界结束；正常路径计算绝不持写锁。

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

# 可控暂停关系计算的双入口交错演示（计算期间另一入口照常提交）
python -I scripts/interleaved_demo.py \
  --base1 http://127.0.0.1:8101 --base2 http://127.0.0.1:8102

# 跨进程并发裁决检查（动态构造合法/冲突竞赛，校验最终不变量）
python -I scripts/concurrency_check.py

# 自动化测试
pytest -q
```

### 可控暂停的交错演示

`scripts/interleaved_demo.py` 针对两个共库 Web 进程演示本次修复的核心行为：

1. 关闭 web1 的「关系计算闸门」（`POST /api/demo/gate`），web1 的裁决停在
   计算阶段——**不持任何数据库锁**；
2. 从 web2 提交无关裁决与名册导入：**立即提交**（脚本打印耗时为证）；
3. 打开闸门后，web1 检测到状态已变化，自动在新快照上重算并提交——
   交错变更不被覆盖、不丢失；
4. 再交错提交一对「分别合法、共同冲突」的合并：恰好一个成功、一个 409；
5. 最后从两个入口分别校验全部分组约束与计数一致性。

Docker Compose 下直接运行（默认端口 8001/8002）：

```bash
docker compose up --build
python -I scripts/interleaved_demo.py
```

浏览器里也可手动复现：两个页签分别打开两个入口，在「并发演示」页签关闭
本进程闸门（或设置单次裁决的计算暂停秒数 `demo_pause_seconds`，0–30），
提交裁决后切到另一页签操作，再回来打开闸门。

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
| GET/POST | `/api/demo/gate` | 演示控制：查看 / 开关本进程的关系计算闸门 |
| GET | `/healthz` | 健康检查 |

> 裁决与撤销另接受可选字段 `demo_pause_seconds`（0–30）：让本次请求的
> 关系计算阶段暂停指定秒数（不持任何锁），用于交错演示。

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
meta(id PK CHECK(id=1), records_version, edges_version)                          -- 乐观并发版本计数
```

- 裁决历史只追加：撤销是把 `active` 置 0 并记录 `revoked_at`，不删除历史行。
- 分组表是当前生效分区的物化结果；每次裁决合并或撤销重算后，在同一写事务内
  与裁决行一起更新（构成未变的分组保留原 group_id）。
- `meta` 版本计数器支撑「计算不持锁」的乐观并发：导入递增 `records_version`，
  裁决/撤销递增 `edges_version`；提交前复核版本，变化则回滚并在新快照上重算。

## 七、目录结构

```text
app/
  db.py            # 连接管理：只读连接 + 短写事务、WAL/schema、meta 版本计数
  services.py      # 领域逻辑：快照→无锁计算→版本校验提交；导入、裁决、撤销、导出
  main.py          # FastAPI 路由、409 异常处理、演示闸门端点、生命周期
  static/index.html# 浏览器单页工作台（原生 JS，无构建步骤，含并发演示页签）
sample_data/       # 虚构样例 CSV 与演练说明
scripts/demo.py              # 双进程端到端演示
scripts/interleaved_demo.py  # 可控暂停关系计算的双入口交错演示
scripts/concurrency_check.py # 跨 OS 进程并发竞赛与不变量校验
tests/test_api.py  # 18 个 pytest 用例（约束/撤销/快照/并发/回滚/计算不持锁）
docker-compose.yml # web1 + web2 共库 + 一次性 seed 导入
Dockerfile
```
