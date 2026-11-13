# -*- coding: utf-8 -*-
"""FastAPI 入口：文件上传/注册、元数据、裁剪计划预览、查询执行与运行历史。"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .compare import router as compare_router
from .filters import GroupFilter, LeafFilter, collect_errors, compile_filter
from .parquet_store import FileStore, build_plan, execute_query, read_metadata

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
UPLOAD_DIR = DATA_DIR / "uploads"
SAMPLE_DIR = DATA_DIR / "samples"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Parquet 行组裁剪实验台", version="1.0.0")
store = FileStore()
app.include_router(compare_router)


class QueryRequest(BaseModel):
    filter: Optional[dict] = None
    pruning: bool = True
    preview_limit: int = Field(default=100, ge=1, le=1000)


class PlanRequest(BaseModel):
    filter: dict


def _validate_filter(file_id: str, payload: dict) -> tuple[object, object]:
    entry = store.get(file_id)
    import pyarrow.parquet as pq
    schema = pq.ParquetFile(entry.path).schema_arrow
    if payload.get("kind") == "group":
        model = GroupFilter.model_validate(payload)
    else:
        model = LeafFilter.model_validate(payload)
    compiled = compile_filter(model, schema)
    errors = collect_errors(compiled)
    if errors:
        raise HTTPException(status_code=400, detail={"message": "筛选条件类型错误", "errors": errors})
    return entry, compiled


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/files")
def list_files():
    return {"files": store.list()}


@app.post("/api/files/upload")
async def upload_file(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".parquet"):
        raise HTTPException(status_code=400, detail={"message": "只接受 .parquet 文件"})
    # 上传文件视为不可信数据：独立目录存放，文件名做净化，只交给 pyarrow 解析
    safe_name = Path(file.filename).name.replace("\x00", "")
    target = UPLOAD_DIR / f"{os.urandom(8).hex()}_{safe_name}"
    with open(target, "wb") as out:
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            out.write(chunk)
    try:
        read_metadata(str(target))  # 校验能否作为 parquet 打开
    except Exception as exc:  # noqa: BLE001
        target.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail={"message": f"无法解析为 Parquet 文件: {exc}"})
    entry = store.register(str(target), safe_name, source="upload")
    return {"file_id": entry.file_id, "metadata": read_metadata(entry.path)}


@app.get("/api/samples")
def list_samples():
    names = sorted(p.name for p in SAMPLE_DIR.glob("*.parquet"))
    return {"samples": [
        {"name": n, "size": (SAMPLE_DIR / n).stat().st_size} for n in names]}


@app.post("/api/files/register-sample/{name}")
def register_sample(name: str):
    path = SAMPLE_DIR / name
    if not path.is_file():
        raise HTTPException(status_code=404, detail={"message": f"样本 {name} 不存在"})
    entry = store.register(str(path), name, source="sample")
    return {"file_id": entry.file_id, "metadata": read_metadata(entry.path)}


@app.get("/api/files/{file_id}/metadata")
def metadata(file_id: str):
    entry = store.get(file_id)
    return read_metadata(entry.path)


@app.post("/api/files/{file_id}/plan")
def plan(file_id: str, req: PlanRequest):
    entry, compiled = _validate_filter(file_id, req.filter)
    result = build_plan(entry.path, compiled)
    result["file_id"] = file_id
    return result


@app.post("/api/files/{file_id}/query")
def query(file_id: str, req: QueryRequest):
    entry = store.get(file_id)
    try:
        run = execute_query(
            entry, entry.path, req.filter, pruning=req.pruning,
            preview_limit=req.preview_limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"message": str(exc)})
    run["file_id"] = file_id
    return run


@app.get("/api/files/{file_id}/runs")
def runs(file_id: str):
    entry = store.get(file_id)
    return {"runs": entry.runs}


@app.get("/api/files/{file_id}/runs/{run_id}")
def get_run(file_id: str, run_id: str):
    entry = store.get(file_id)
    for r in entry.runs:
        if r["run_id"] == run_id:
            return r
    raise HTTPException(status_code=404, detail={"message": "运行记录不存在"})


@app.get("/")
def index():
    return FileResponse(str(BASE_DIR / "app" / "static" / "index.html"))


app.mount("/static", StaticFiles(directory=str(BASE_DIR / "app" / "static")), name="static")
