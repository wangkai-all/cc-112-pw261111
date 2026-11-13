# -*- coding: utf-8 -*-
"""Parquet 文件注册、元数据读取、裁剪计划与带真实磁盘读取计量的执行引擎。

读取统计的两个来源（页面均会标明口径，绝不用文件总大小冒充读取量）：
- plan_bytes：明确的读取计划，等于候选行组中被选中列块的 total_compressed_size 之和；
- measured_bytes：执行记录，由 CountingRawIO 对底层文件描述符的实际 read() 字节计数，
  含 Parquet footer 的读取（footer 长度以真实读取为准，单列统计），与列块数据分别汇总。
"""

from __future__ import annotations

import datetime
import hashlib
import io
import json
import math
import os
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Optional

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .filters import (
    CompiledLeaf,
    collect_errors,
    compile_filter,
    evaluate_rowgroup,
    type_family,
)


# ---------------------------------------------------------------------------
# 真实磁盘读取计数：包装裸文件对象，统计 read 系统调用返回的字节数
# ---------------------------------------------------------------------------

class CountingRawIO(io.RawIOBase):
    """只做转发与计数，不做缓冲，保证数字等于底层实际读取字节。"""

    def __init__(self, path: str):
        self._f = open(path, "rb")
        self.bytes_read = 0
        self.read_calls = 0

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def readinto(self, b):
        data = self._f.read(len(b))
        n = len(data)
        self.bytes_read += n
        self.read_calls += 1
        b[:n] = data
        return n

    def seek(self, offset, whence=0):
        return self._f.seek(offset, whence)

    def tell(self):
        return self._f.tell()

    def close(self):
        self._f.close()
        super().close()


# ---------------------------------------------------------------------------
# JSON 安全化
# ---------------------------------------------------------------------------

def json_safe(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (datetime.datetime, datetime.date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    if isinstance(v, float):
        # 标准 JSON 不支持 NaN/Infinity，输出为显式字符串标记，前端原样展示
        if math.isnan(v):
            return "NaN"
        if math.isinf(v):
            return "Infinity" if v > 0 else "-Infinity"
        return v
    if isinstance(v, (int, bool, str)):
        return v
    return str(v)


# ---------------------------------------------------------------------------
# 文件注册
# ---------------------------------------------------------------------------

@dataclass
class FileEntry:
    file_id: str
    path: str
    name: str
    source: str  # sample | upload
    created_at: str
    runs: list[dict] = field(default_factory=list)


class FileStore:
    def __init__(self) -> None:
        self._files: dict[str, FileEntry] = {}

    def register(self, path: str, name: str, source: str) -> FileEntry:
        file_id = uuid.uuid4().hex[:12]
        entry = FileEntry(
            file_id=file_id, path=path, name=name, source=source,
            created_at=datetime.datetime.now().isoformat(timespec="seconds"))
        self._files[file_id] = entry
        return entry

    def get(self, file_id: str) -> FileEntry:
        if file_id not in self._files:
            raise KeyError(file_id)
        return self._files[file_id]

    def list(self) -> list[dict]:
        return [
            {"file_id": e.file_id, "name": e.name, "source": e.source,
             "created_at": e.created_at, "size": os.path.getsize(e.path),
             "runs": len(e.runs)}
            for e in self._files.values()
        ]


# ---------------------------------------------------------------------------
# 元数据
# ---------------------------------------------------------------------------

def read_metadata(path: str) -> dict:
    pf = pq.ParquetFile(path)
    md = pf.metadata
    arrow_schema = pf.schema_arrow

    columns = []
    for idx, fld in enumerate(arrow_schema):
        columns.append({
            "name": fld.name,
            "arrow_type": str(fld.type),
            "nullable": fld.nullable,
            "family": type_family(fld.type),
        })

    row_groups = []
    for rg_idx in range(md.num_row_groups):
        rg = md.row_group(rg_idx)
        col_chunks = []
        for col_idx in range(rg.num_columns):
            chunk = rg.column(col_idx)
            st = chunk.statistics
            stat = None
            if st is not None:
                stat = {
                    "has_min_max": st.has_min_max,
                    "min": json_safe(st.min) if st.has_min_max else None,
                    "max": json_safe(st.max) if st.has_min_max else None,
                    "null_count": st.null_count,
                    "num_values": st.num_values,
                    "physical_type": st.physical_type,
                }
            col_chunks.append({
                "name": chunk.path_in_schema.split(".")[0],
                "path_in_schema": chunk.path_in_schema,
                "physical_type": chunk.physical_type,
                "compressed_size": chunk.total_compressed_size,
                "uncompressed_size": chunk.total_uncompressed_size,
                "statistics": stat,
            })
        row_groups.append({
            "row_group_index": rg_idx,
            "num_rows": rg.num_rows,
            "total_byte_size": rg.total_byte_size,
            "sorting_columns": [
                {"column_index": sc.column_index,
                 "descending": sc.descending, "nulls_first": sc.nulls_first}
                for sc in (rg.sorting_columns or [])
            ],
            "columns": col_chunks,
        })

    return {
        "file_size": os.path.getsize(path),
        "num_rows": md.num_rows,
        "num_row_groups": md.num_row_groups,
        "num_columns": md.num_columns,
        "created_by": md.created_by,
        "format_version": f"Parquet {md.format_version}",
        "serialized_footer_size": md.serialized_size,
        "columns": columns,
        "row_groups": row_groups,
    }


def _rowgroup_stats_map(pf: pq.ParquetFile, rg_idx: int) -> dict[str, Any]:
    rg = pf.metadata.row_group(rg_idx)
    out: dict[str, Any] = {}
    for col_idx in range(rg.num_columns):
        chunk = rg.column(col_idx)
        out[chunk.path_in_schema.split(".")[0]] = chunk.statistics
    return out


# ---------------------------------------------------------------------------
# 裁剪计划（不执行读取，只依据 footer 统计信息）
# ---------------------------------------------------------------------------

def build_plan(path: str, compiled: Any, selected_columns: Optional[list[str]] = None) -> dict:
    pf = pq.ParquetFile(path)
    md = pf.metadata
    if selected_columns is None:
        selected_columns = [f.name for f in pf.schema_arrow]

    decisions = []
    kept, excluded = [], []
    plan_bytes = 0
    for rg_idx in range(md.num_row_groups):
        stats_map = _rowgroup_stats_map(pf, rg_idx)
        is_excluded, trace = evaluate_rowgroup(compiled, stats_map)
        rg = md.row_group(rg_idx)
        rg_plan_bytes = 0
        for col_idx in range(rg.num_columns):
            chunk = rg.column(col_idx)
            if chunk.path_in_schema.split(".")[0] in selected_columns:
                rg_plan_bytes += chunk.total_compressed_size
        decisions.append({
            "row_group_index": rg_idx,
            "num_rows": rg.num_rows,
            "excluded": is_excluded,
            "decision": "排除" if is_excluded else "保留",
            "plan_bytes_if_kept": rg_plan_bytes,
            "trace": trace,
        })
        if is_excluded:
            excluded.append(rg_idx)
        else:
            kept.append(rg_idx)
            plan_bytes += rg_plan_bytes

    return {
        "num_row_groups": md.num_row_groups,
        "kept_row_groups": kept,
        "excluded_row_groups": excluded,
        "kept_rows": sum(md.row_group(i).num_rows for i in kept),
        "plan_bytes": plan_bytes,
        "decisions": decisions,
        "selected_columns": selected_columns,
    }


# ---------------------------------------------------------------------------
# 行级过滤（真实查询语义，三值逻辑：NULL 不满足筛选）
# ---------------------------------------------------------------------------

def _build_scalar_and_array(leaf: CompiledLeaf, column: pa.ChunkedArray):
    fam = leaf.family
    if pa.types.is_dictionary(leaf.arrow_type):
        column = column.cast(leaf.arrow_type.value_type)
        arrow_type = leaf.arrow_type.value_type
    else:
        arrow_type = leaf.arrow_type

    if fam == "decimal":
        v_dec = Decimal(leaf.value)
        input_scale = max(0, -v_dec.as_tuple().exponent)
        scale = max(arrow_type.scale, input_scale)
        scaled_digits = v_dec.scaleb(scale).as_tuple().digits
        int_digits = len(scaled_digits) - scale
        precision = max(arrow_type.precision, max(int_digits, 0) + scale)
        wide_type = pa.decimal128(min(precision, 38), scale)
        column = column.cast(wide_type)
        return column, pa.scalar(v_dec, type=wide_type)

    if fam == "binary":
        return column, pa.scalar(leaf.value, type=arrow_type)
    return column, pa.scalar(leaf.value, type=arrow_type)


_PC_OPS = {
    "eq": pc.equal,
    "ne": pc.not_equal,
    "gt": pc.greater,
    "ge": pc.greater_equal,
    "lt": pc.less,
    "le": pc.less_equal,
}


def _leaf_mask(leaf: CompiledLeaf, table: pa.Table):
    column = table.column(leaf.column)
    column, scalar = _build_scalar_and_array(leaf, column)
    mask = _PC_OPS[leaf.op](column, scalar)
    # SQL 三值逻辑：比较结果为 NULL 的行不满足筛选
    return pc.fill_null(mask, False)


def evaluate_table_mask(table: pa.Table, compiled: Any):
    if isinstance(compiled, CompiledLeaf):
        return _leaf_mask(compiled, table)
    masks = [evaluate_table_mask(table, ch) for ch in compiled.children]
    acc = masks[0]
    for m in masks[1:]:
        acc = pc.and_(acc, m) if compiled.logic == "and" else pc.or_(acc, m)
    return acc


# ---------------------------------------------------------------------------
# 规范化与结果摘要（跨变体一致性比对用，与行顺序无关）
# ---------------------------------------------------------------------------

def _normalize_pylist(rows: list[dict]) -> list[dict]:
    out = []
    for row in rows:
        norm = {}
        for k, v in row.items():
            if isinstance(v, float):
                if math.isnan(v):
                    v = "__nan__"
                elif math.isinf(v):
                    v = "__inf__" if v > 0 else "__-inf__"
            elif isinstance(v, bytes):
                v = v.decode("utf-8", errors="replace")
            norm[k] = v
        out.append(norm)
    return out


def canonical_digest(table: pa.Table) -> tuple[str, int]:
    """返回 (sha256, 行数)。按全部列排序后序列化，行顺序不影响摘要。"""
    if table.num_rows == 0:
        payload = json.dumps(
            {"schema": str(table.schema), "rows": []},
            ensure_ascii=False, sort_keys=True).encode("utf-8")
        return hashlib.sha256(payload).hexdigest(), 0

    # dictionary 列不能直接排序，先转成值类型
    casted = table
    for fld in table.schema:
        if pa.types.is_dictionary(fld.type):
            casted = casted.set_column(
                table.schema.get_field_index(fld.name),
                fld.name,
                casted.column(fld.name).cast(fld.type.value_type))

    sort_keys = [(fld.name, "ascending") for fld in casted.schema]
    try:
        idx = pc.sort_indices(casted, sort_keys=sort_keys, null_placement="at_end")
        ordered = casted.take(idx)
    except Exception:  # noqa: BLE001 - 个别类型排序失败时退化为按行 JSON 排序
        rows = casted.to_pylist()
        rows.sort(key=lambda r: json.dumps(r, ensure_ascii=False, default=str, sort_keys=True))
        ordered_rows = rows
    else:
        ordered_rows = ordered.to_pylist()

    normalized = _normalize_pylist(ordered_rows)
    payload = json.dumps(
        {"schema": str(casted.schema), "rows": normalized},
        ensure_ascii=False, sort_keys=True, default=str,
        separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest(), len(normalized)


# ---------------------------------------------------------------------------
# 执行：裁剪模式 / 完整扫描
# ---------------------------------------------------------------------------

def _filter_summary(compiled: Any) -> dict:
    if isinstance(compiled, CompiledLeaf):
        return {"kind": "leaf", "column": compiled.column, "op": compiled.op,
                "value": json_safe(compiled.raw_value)}
    return {"kind": "group", "logic": compiled.logic,
            "children": [_filter_summary(ch) for ch in compiled.children]}


def execute_query(
    entry: FileEntry,
    path: str,
    filter_tree: Optional[dict],
    pruning: bool,
    preview_limit: int = 100,
) -> dict:
    # 1. 编译筛选（类型错误在 API 层已拦截，这里再兜底一次）
    from .filters import GroupFilter, LeafFilter

    compiled = None
    if filter_tree is not None:
        model = GroupFilter.model_validate(filter_tree) if filter_tree.get("kind") == "group" \
            else LeafFilter.model_validate(filter_tree)
        pf0 = pq.ParquetFile(path)
        compiled = compile_filter(model, pf0.schema_arrow)
        errors = collect_errors(compiled)
        if errors:
            raise ValueError("；".join(errors))

    # 2. 计划（用于对照与候选行组说明）
    plan = build_plan(path, compiled) if compiled is not None else None

    # 3. 真实执行 + 计数
    counter = CountingRawIO(path)
    per_row_group = []
    with pa.PythonFile(counter, mode="r") as pyfile:
        pf = pq.ParquetFile(pyfile)
        footer_bytes = counter.bytes_read  # 打开 ParquetFile 后 footer 已读完

        if pruning and compiled is not None:
            target_rgs = plan["kept_row_groups"]
        else:
            target_rgs = list(range(pf.metadata.num_row_groups))

        data_bytes_before = counter.bytes_read
        tables = []
        for rg_idx in target_rgs:
            before = counter.bytes_read
            tbl = pf.read_row_group(rg_idx)
            delta = counter.bytes_read - before
            per_row_group.append({
                "row_group_index": rg_idx,
                "measured_bytes": delta,
                "planned_bytes": sum(
                    pf.metadata.row_group(rg_idx).column(c).total_compressed_size
                    for c in range(pf.metadata.row_group(rg_idx).num_columns)),
                "num_rows": pf.metadata.row_group(rg_idx).num_rows,
            })
            tables.append(tbl)
        data_bytes = counter.bytes_read - data_bytes_before

        if tables:
            full_table = pa.concat_tables(tables)
        else:
            full_table = pa.Table.from_batches([], schema=pf.schema_arrow)

    # 4. 行级过滤
    if compiled is not None:
        mask = evaluate_table_mask(full_table, compiled)
        result_table = full_table.filter(mask)
    else:
        result_table = full_table

    digest, result_rows = canonical_digest(result_table)
    preview = [
        {k: json_safe(v) for k, v in row.items()}
        for row in result_table.slice(0, preview_limit).to_pylist()
    ]

    run = {
        "run_id": uuid.uuid4().hex[:12],
        "timestamp": datetime.datetime.now().isoformat(timespec="seconds"),
        "mode": "pruning" if (pruning and compiled is not None) else "full_scan",
        "filter": _filter_summary(compiled) if compiled is not None else None,
        "candidate_row_groups": (
            plan["kept_row_groups"] if (pruning and plan is not None)
            else list(range(pf.metadata.num_row_groups))),
        "excluded_row_groups": (
            plan["excluded_row_groups"] if (pruning and plan is not None) else []),
        "read_row_groups": target_rgs,
        "measured_total_bytes": counter.bytes_read,
        "measured_footer_bytes": footer_bytes,
        "measured_data_bytes": data_bytes,
        "planned_data_bytes": plan["plan_bytes"] if plan else sum(
            c["planned_bytes"] for c in per_row_group),
        "per_row_group": per_row_group,
        "scanned_rows": full_table.num_rows,
        "result_rows": result_rows,
        "result_digest": digest,
        "preview": preview,
        "preview_truncated": result_rows > preview_limit,
    }
    entry.runs.append(run)
    return run
