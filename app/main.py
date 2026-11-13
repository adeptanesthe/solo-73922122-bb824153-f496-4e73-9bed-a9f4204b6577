"""FastAPI application: REST endpoints plus a small browser UI."""

from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from typing import Literal

from fastapi import FastAPI, File, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, Response
from pydantic import BaseModel, Field

from . import services
from .db import DEFAULT_DB_PATH, initialize
from .services import ConflictError


class AdjudicationIn(BaseModel):
    record_a: int = Field(..., gt=0, description="第一条记录 ID")
    record_b: int = Field(..., gt=0, description="第二条记录 ID")
    decision: Literal["same", "different"]
    demo_pause_seconds: float = Field(
        0.0, ge=0.0, le=30.0,
        description="演示用：让本次请求的关系计算阶段暂停指定秒数（不持任何锁）")


class RevokeIn(BaseModel):
    adjudication_id: int = Field(..., gt=0)
    demo_pause_seconds: float = Field(
        0.0, ge=0.0, le=30.0,
        description="演示用：让本次请求的关系计算阶段暂停指定秒数（不持任何锁）")


class GateIn(BaseModel):
    open: bool = Field(..., description="true 打开计算闸门，false 关闭（暂停本进程关系计算）")


class PathIn(BaseModel):
    record_a: int = Field(..., gt=0)
    record_b: int = Field(..., gt=0)


class ImportRawIn(BaseModel):
    raw: str = Field(..., min_length=1, description="CSV 文本内容")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # Idempotent; every replica runs it. CREATE TABLE IF NOT EXISTS is
    # serialised by SQLite, so first-writer-wins is safe.
    initialize()
    yield


app = FastAPI(
    title="随访入组名册关联裁决",
    description="人工核定跨队列记录是否属于同一参与者的关联裁决服务",
    version="1.0.0",
    lifespan=lifespan,
)


@app.exception_handler(ConflictError)
def _conflict_handler(_request, exc: ConflictError):
    # Whole-request rejection: 409 carries the conflicting records and paths.
    return Response(
        content=json.dumps(
            {"detail": str(exc), **exc.details}, ensure_ascii=False
        ),
        status_code=409,
        media_type="application/json",
    )


@app.get("/healthz")
def healthz():
    return {"status": "ok", "db": os.path.abspath(DEFAULT_DB_PATH)}


# --------------------------------------------------------------------------- #
# Import / export
# --------------------------------------------------------------------------- #

@app.post("/api/import", summary="粘贴 CSV 文本导入（原记录不修改）")
def import_csv_text(body: ImportRawIn):
    # ConflictError propagates to the registered 409 handler uniformly.
    return services.import_csv(body.raw)


@app.post("/api/import/file", summary="上传 CSV 文件导入（原记录不修改）")
async def import_csv_file(file: UploadFile = File(...)):
    content = await file.read()
    return services.import_csv(content)


@app.get("/api/export", summary="下载同一已提交快照的来源→分组映射 CSV")
def export_csv():
    body = services.export_mapping_csv()
    # Attachment download; the query reads one committed snapshot.
    return Response(
        content=body,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition":
                 'attachment; filename="source_group_mapping.csv"'},
    )


# --------------------------------------------------------------------------- #
# Reads
# --------------------------------------------------------------------------- #

@app.get("/api/records", summary="记录列表（含当前分组）")
def get_records():
    return services.list_records()


@app.get("/api/groups", summary="当前分组视图")
def get_groups():
    return services.list_groups()


@app.get("/api/adjudications", summary="裁决列表（默认仅生效中）")
def get_adjudications(active_only: bool = True):
    return services.list_adjudications(active_only=active_only)


@app.get("/api/stats", summary="计数概览")
def get_stats():
    return services.stats()


@app.post("/api/same-path", summary="查询两条记录的同人连接路径")
def post_same_path(body: PathIn):
    return services.same_path(body.record_a, body.record_b)


# --------------------------------------------------------------------------- #
# Mutations
# --------------------------------------------------------------------------- #

@app.post("/api/adjudicate", summary="提交同人/不同人裁决（冲突则整次拒绝）")
def post_adjudicate(body: AdjudicationIn):
    return services.adjudicate(body.record_a, body.record_b, body.decision,
                               demo_pause_seconds=body.demo_pause_seconds)


@app.post("/api/revoke", summary="撤销裁决并重新拆组")
def post_revoke(body: RevokeIn):
    return services.revoke(body.adjudication_id,
                           demo_pause_seconds=body.demo_pause_seconds)


# --------------------------------------------------------------------------- #
# Demo control: pause / resume this process's relationship computation
# --------------------------------------------------------------------------- #

@app.get("/api/demo/gate", summary="查看本进程关系计算闸门状态（演示控制）")
def get_demo_gate():
    return {"open": services.compute_gate_open()}


@app.post("/api/demo/gate", summary="打开/关闭本进程关系计算闸门（演示控制）")
def set_demo_gate(body: GateIn):
    # Closing the gate parks this process's adjudications/revocations inside
    # their computation phase — with no database lock held — so a second
    # entry point can be shown to keep committing.  Per-process state.
    services.set_compute_gate(body.open)
    return {"open": services.compute_gate_open()}


# --------------------------------------------------------------------------- #
# Browser UI
# --------------------------------------------------------------------------- #

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def index():
    ui_path = os.path.join(os.path.dirname(__file__), "static", "index.html")
    return FileResponse(ui_path)
