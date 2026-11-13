# -*- coding: utf-8 -*-
"""端到端测试：API 可用性、裁剪 vs 完整扫描结果一致、读取计量口径正确。"""

import datetime
import decimal
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from app.main import app, store
from app.parquet_store import execute_query


@pytest.fixture()
def client():
    store._files.clear()
    return TestClient(app)


@pytest.fixture(scope="module")
def sample_variants():
    base = os.path.join(os.path.dirname(__file__), "..", "data", "samples")
    return {
        "unsorted_2000": os.path.join(base, "orders_unsorted_rg2000.parquet"),
        "sorted_date": os.path.join(base, "orders_sorted_date_rg2000.parquet"),
        "sorted_amount": os.path.join(base, "orders_sorted_amount_rg2000.parquet"),
        "sorted_date_small": os.path.join(base, "orders_sorted_date_rg500.parquet"),
        "edge": os.path.join(base, "edge_cases_rg3.parquet"),
        "long": os.path.join(base, "long_strings.parquet"),
    }


def _register(client, path, name):
    with open(path, "rb") as f:
        resp = client.post(
            "/api/files/upload",
            files={"file": (name, f, "application/octet-stream")})
    assert resp.status_code == 200, resp.text
    return resp.json()["file_id"]


def test_health_and_upload_metadata(client, sample_variants):
    fid = _register(client, sample_variants["sorted_date"], "orders.parquet")
    resp = client.get(f"/api/files/{fid}/metadata")
    assert resp.status_code == 200
    md = resp.json()
    assert md["num_rows"] == 12000
    assert md["num_row_groups"] == 6
    cols = {c["name"]: c for c in md["columns"]}
    assert cols["order_date"]["family"] == "date"
    assert cols["amount"]["family"] == "decimal"
    assert cols["level"]["family"] == "string"
    # 每个行组的每列都带统计块
    for rg in md["row_groups"]:
        for c in rg["columns"]:
            assert c["statistics"] is not None


def test_upload_rejects_non_parquet(client, tmp_path):
    bad = tmp_path / "x.parquet"
    bad.write_bytes(b"not a parquet file")
    with open(bad, "rb") as f:
        resp = client.post("/api/files/upload",
                           files={"file": ("x.parquet", f, "application/octet-stream")})
    assert resp.status_code == 400


def test_plan_endpoint_explains_decisions(client, sample_variants):
    fid = _register(client, sample_variants["sorted_date"], "s.parquet")
    payload = {"filter": {
        "kind": "leaf", "column": "order_date", "op": "ge",
        "value": "2024-10-01"}}
    resp = client.post(f"/api/files/{fid}/plan", json=payload)
    assert resp.status_code == 200
    plan = resp.json()
    assert plan["excluded_row_groups"]  # 日期排序后早期行组应被排除
    for d in plan["decisions"]:
        assert d["trace"]["reason"]
        assert d["trace"]["decision"] in ("保留", "排除")
    # 排序文件裁剪效果明显优于不排序文件
    fid2 = _register(client, sample_variants["unsorted_2000"], "u.parquet")
    plan2 = client.post(f"/api/files/{fid2}/plan", json=payload).json()
    assert len(plan["excluded_row_groups"]) >= len(plan2["excluded_row_groups"])


def test_type_mismatch_returns_400(client, sample_variants):
    fid = _register(client, sample_variants["edge"], "e.parquet")
    resp = client.post(f"/api/files/{fid}/plan", json={"filter": {
        "kind": "leaf", "column": "id", "op": "eq", "value": "not-int"}})
    assert resp.status_code == 400
    assert "errors" in resp.json()["detail"]


def test_pruning_and_full_scan_equivalence(client, sample_variants):
    filters = [
        {"kind": "leaf", "column": "order_date", "op": "ge", "value": "2024-07-01"},
        {"kind": "leaf", "column": "amount", "op": "gt", "value": "5000.00"},
        {"kind": "group", "logic": "and", "children": [
            {"kind": "leaf", "column": "order_date", "op": "lt",
             "value": "2024-09-01"},
            {"kind": "leaf", "column": "amount", "op": "ge", "value": "100.00"},
            {"kind": "leaf", "column": "level", "op": "eq", "value": "金卡"},
        ]},
        {"kind": "group", "logic": "or", "children": [
            {"kind": "leaf", "column": "region", "op": "eq", "value": "海外"},
            {"kind": "leaf", "column": "amount", "op": "lt", "value": "50.00"},
        ]},
        {"kind": "group", "logic": "and", "children": [
            {"kind": "leaf", "column": "discount", "op": "le", "value": 0.1},
            {"kind": "group", "logic": "or", "children": [
                {"kind": "leaf", "column": "level", "op": "eq", "value": "钻石"},
                {"kind": "leaf", "column": "order_id", "op": "gt", "value": 110000},
            ]},
        ]},
    ]
    # 订单样本（7 列）上的组合筛选
    for variant_key in ("unsorted_2000", "sorted_date", "sorted_amount",
                        "sorted_date_small"):
        fid = _register(client, sample_variants[variant_key], f"{variant_key}.parquet")
        for flt in filters:
            r_prune = client.post(f"/api/files/{fid}/query",
                                  json={"filter": flt, "pruning": True}).json()
            r_full = client.post(f"/api/files/{fid}/query",
                                 json={"filter": flt, "pruning": False}).json()
            assert r_prune["result_rows"] == r_full["result_rows"], (variant_key, flt)
            assert r_prune["result_digest"] == r_full["result_digest"], (variant_key, flt)
            assert len(r_full["read_row_groups"]) == len(r_full["candidate_row_groups"])
            assert len(r_prune["read_row_groups"]) <= len(r_full["read_row_groups"])

    # 边界文件（5 列）上的等价性，覆盖全 NULL 行组与单值边界行组
    edge_filters = [
        {"kind": "leaf", "column": "order_date", "op": "ge", "value": "2024-05-01"},
        {"kind": "leaf", "column": "amount", "op": "gt", "value": "15.00"},
        {"kind": "group", "logic": "and", "children": [
            {"kind": "leaf", "column": "order_date", "op": "lt", "value": "2024-06-01"},
            {"kind": "leaf", "column": "amount", "op": "ge", "value": "10.00"},
        ]},
        {"kind": "group", "logic": "or", "children": [
            {"kind": "leaf", "column": "label", "op": "eq", "value": "不存在的标签"},
            {"kind": "leaf", "column": "order_id", "op": "lt", "value": 5},
        ]},
        {"kind": "group", "logic": "and", "children": [
            {"kind": "leaf", "column": "score", "op": "le", "value": 3.0},
            {"kind": "group", "logic": "or", "children": [
                {"kind": "leaf", "column": "label", "op": "eq", "value": "短"},
                {"kind": "leaf", "column": "order_id", "op": "gt", "value": 6},
            ]},
        ]},
    ]
    fid = _register(client, sample_variants["edge"], "edge.parquet")
    for flt in edge_filters:
        r_prune = client.post(f"/api/files/{fid}/query",
                              json={"filter": flt, "pruning": True}).json()
        r_full = client.post(f"/api/files/{fid}/query",
                             json={"filter": flt, "pruning": False}).json()
        assert r_prune["result_rows"] == r_full["result_rows"], flt
        assert r_prune["result_digest"] == r_full["result_digest"], flt
        assert len(r_prune["read_row_groups"]) <= len(r_full["read_row_groups"])


def test_measured_bytes_are_real_io_not_file_size(client, sample_variants):
    fid = _register(client, sample_variants["sorted_date"], "s.parquet")
    file_size = os.path.getsize(sample_variants["sorted_date"])
    flt = {"kind": "leaf", "column": "order_date", "op": "ge", "value": "2024-11-01"}
    run = client.post(f"/api/files/{fid}/query", json={"filter": flt, "pruning": True}).json()
    # 真实计量小于文件总大小，且计划与计量口径分别给出
    assert run["measured_total_bytes"] < file_size
    assert run["measured_data_bytes"] > 0
    assert run["planned_data_bytes"] > 0
    # 逐行组 measured == planned（列块压缩尺寸之和），证明计量可信
    for rg in run["per_row_group"]:
        assert rg["measured_bytes"] == rg["planned_bytes"]
    # 完整扫描的数据读取量大于裁剪模式
    full = client.post(f"/api/files/{fid}/query",
                       json={"filter": flt, "pruning": False}).json()
    assert full["measured_data_bytes"] > run["measured_data_bytes"]


def test_runs_history_recorded(client, sample_variants):
    fid = _register(client, sample_variants["edge"], "e.parquet")
    client.post(f"/api/files/{fid}/query", json={
        "filter": {"kind": "leaf", "column": "order_id", "op": "gt", "value": 2},
        "pruning": True})
    resp = client.get(f"/api/files/{fid}/runs")
    assert resp.status_code == 200
    assert len(resp.json()["runs"]) == 1
    rid = resp.json()["runs"][0]["run_id"]
    one = client.get(f"/api/files/{fid}/runs/{rid}")
    assert one.status_code == 200
    assert one.json()["run_id"] == rid


def test_null_semantics_in_results(client, sample_variants):
    fid = _register(client, sample_variants["edge"], "e.parquet")
    # note/discount 含 NULL：eq 不应返回 NULL 行；ne 也不返回 NULL 行
    run = client.post(f"/api/files/{fid}/query", json={
        "filter": {"kind": "leaf", "column": "label", "op": "eq", "value": "a"},
        "pruning": False}).json()
    assert run["result_rows"] == 1
    run_ne = client.post(f"/api/files/{fid}/query", json={
        "filter": {"kind": "leaf", "column": "label", "op": "ne", "value": "a"},
        "pruning": False}).json()
    assert run_ne["result_rows"] == 6


def test_engine_direct_full_scan_without_filter(sample_variants):
    from app.parquet_store import FileStore
    local = FileStore()
    entry = local.register(sample_variants["edge"], "edge", "sample")
    run = execute_query(entry, sample_variants["edge"], None, pruning=True)
    assert run["result_rows"] == 8
    assert run["mode"] == "full_scan"


def test_compare_variants_consistent(client, sample_variants):
    ids = {}
    for key in ("unsorted_2000", "sorted_date", "sorted_amount", "sorted_date_small"):
        ids[key] = _register(client, sample_variants[key], f"{key}.parquet")
    flt = {"kind": "group", "logic": "and", "children": [
        {"kind": "leaf", "column": "order_date", "op": "ge", "value": "2024-06-01"},
        {"kind": "leaf", "column": "amount", "op": "lt", "value": "6000.00"},
    ]}
    resp = client.post("/api/compare", json={
        "file_ids": list(ids.values()), "filter": flt, "pruning": True})
    assert resp.status_code == 200
    body = resp.json()
    assert body["consistent_across_variants"] is True
    assert body["pruning_matches_full_scan"] is True
    # 日期排序变体在日期过滤上应比未排序变体排除更多行组
    by_name = {r["name"]: r for r in body["pruning_runs"]}
    assert by_name["sorted_date.parquet"]["num_excluded_groups"] >= \
        by_name["unsorted_2000.parquet"]["num_excluded_groups"]
    # 所有读取计量都小于各自文件大小
    import os
    for r in body["pruning_runs"]:
        assert r["measured_data_bytes"] < os.path.getsize(sample_variants[
            {v: k for k, v in ids.items()}[r["file_id"]]])


def test_compare_requires_two_files(client, sample_variants):
    fid = _register(client, sample_variants["edge"], "e.parquet")
    resp = client.post("/api/compare", json={"file_ids": [fid], "filter": None})
    assert resp.status_code == 400
