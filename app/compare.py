# -*- coding: utf-8 -*-
"""跨文件（同一数据集的不同 Parquet 变体）对比：候选行组、真实读取量、结果一致性。"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .parquet_store import execute_query

router = APIRouter(prefix="/api/compare", tags=["compare"])


class CompareRequest(BaseModel):
    file_ids: list[str]
    filter: dict | None = None
    pruning: bool = True


def _run_one(store, file_id: str, req: CompareRequest) -> dict:
    entry = store.get(file_id)
    run = execute_query(entry, entry.path, req.filter, pruning=req.pruning)
    return {
        "file_id": file_id,
        "name": entry.name,
        "mode": run["mode"],
        "candidate_row_groups": run["candidate_row_groups"],
        "excluded_row_groups": run["excluded_row_groups"],
        "read_row_groups": run["read_row_groups"],
        "num_candidate_groups": len(run["candidate_row_groups"]),
        "num_excluded_groups": len(run["excluded_row_groups"]),
        "measured_total_bytes": run["measured_total_bytes"],
        "measured_footer_bytes": run["measured_footer_bytes"],
        "measured_data_bytes": run["measured_data_bytes"],
        "planned_data_bytes": run["planned_data_bytes"],
        "scanned_rows": run["scanned_rows"],
        "result_rows": run["result_rows"],
        "result_digest": run["result_digest"],
        "run_id": run["run_id"],
    }


@router.post("")
def compare(req: CompareRequest) -> dict:
    # 复用 main 中的全局文件注册表
    from .main import store as store
    if len(req.file_ids) < 2:
        raise HTTPException(status_code=400, detail={"message": "对比至少选择两个文件"})
    results = [_run_one(store, fid, req) for fid in req.file_ids]

    digests = {r["result_digest"] for r in results}
    row_counts = {r["result_rows"] for r in results}
    consistent = len(digests) == 1 and len(row_counts) == 1

    # 再对每个文件跑一次完整扫描，验证裁剪结果与完整扫描完全一致
    full_scan = [_run_one(store, fid, CompareRequest(
        file_ids=req.file_ids, filter=req.filter, pruning=False))
        for fid in req.file_ids]
    pruning_matches_full = all(
        p["result_digest"] == f["result_digest"] and p["result_rows"] == f["result_rows"]
        for p, f in zip(results, full_scan))

    min_data = min(r["measured_data_bytes"] for r in results)
    for r in results:
        r["saving_vs_worst_pct"] = round(
            100 * (1 - r["measured_data_bytes"] /
                   max(x["measured_data_bytes"] for x in results)), 1)

    return {
        "consistent_across_variants": consistent,
        "pruning_matches_full_scan": pruning_matches_full,
        "shared_result_rows": results[0]["result_rows"] if consistent else None,
        "shared_digest": results[0]["result_digest"] if consistent else None,
        "pruning_runs": results,
        "full_scan_runs": full_scan,
        "min_measured_data_bytes": min_data,
    }
