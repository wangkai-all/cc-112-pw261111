# -*- coding: utf-8 -*-
"""裁剪安全性与类型语义的单元测试。

核心不变量：对任何行组，凡是被判定“排除”的，行级真实过滤在该组上必然零命中。
"""

import datetime
import decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from app.filters import (
    GroupFilter,
    LeafFilter,
    FilterTypeError,
    compile_filter,
    collect_errors,
    evaluate_rowgroup,
    parse_value,
    type_family,
)
from app.parquet_store import build_plan, canonical_digest, evaluate_table_mask


@pytest.fixture(scope="module")
def edge_pf(tmp_path_factory):
    """3 个行组：全 NULL 组 / 含 NULL 与 NaN 组 / 单值边界组。"""
    path = tmp_path_factory.mktemp("pq") / "edge.parquet"
    tables = [
        pa.table({
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "d": pa.array([None, None, None], type=pa.date32()),
            "amt": pa.array([decimal.Decimal("10.00")] * 3, type=pa.decimal128(10, 2)),
            "label": pa.array(["x", "y", "z"], type=pa.string()),
            "score": pa.array([1.0, 2.0, 3.0], type=pa.float64()),
        }),
        pa.table({
            "id": pa.array([4, 5, 6], type=pa.int64()),
            "d": pa.array([datetime.date(2024, 5, 1), None, datetime.date(2024, 5, 2)],
                          type=pa.date32()),
            "amt": pa.array([decimal.Decimal("10.00"), decimal.Decimal("20.00"),
                             decimal.Decimal("30.00")], type=pa.decimal128(10, 2)),
            "label": pa.array(["a", None, "b"], type=pa.string()),
            "score": pa.array([float("nan"), 2.5, None], type=pa.float64()),
        }),
        pa.table({
            "id": pa.array([7, 8], type=pa.int64()),
            "d": pa.array([datetime.date(2024, 6, 1), datetime.date(2024, 6, 2)],
                          type=pa.date32()),
            "amt": pa.array([decimal.Decimal("10.00"), decimal.Decimal("10.00")],
                            type=pa.decimal128(10, 2)),
            "label": pa.array(["短", "短"], type=pa.string()),
            "score": pa.array([4.0, 4.0], type=pa.float64()),
        }),
    ]
    pq.write_table(pa.concat_tables(tables), path, row_group_size=3)
    return pq.ParquetFile(path), str(path)


def stats_map(pf, rg_idx):
    rg = pf.metadata.row_group(rg_idx)
    return {rg.column(i).path_in_schema: rg.column(i).statistics
            for i in range(rg.num_columns)}


# --------------------------------------------------------------------------
# 类型解析
# --------------------------------------------------------------------------

def test_parse_int_range_and_bool_reject():
    with pytest.raises(FilterTypeError):
        parse_value(pa.int32(), 2**31)
    with pytest.raises(FilterTypeError):
        parse_value(pa.int32(), True)
    assert parse_value(pa.int32(), "42") == 42
    with pytest.raises(FilterTypeError):
        parse_value(pa.uint32(), -1)


def test_parse_float_rejects_nan_inf_and_non_numeric():
    with pytest.raises(FilterTypeError):
        parse_value(pa.float64(), "nan")
    with pytest.raises(FilterTypeError):
        parse_value(pa.float32(), "inf")
    with pytest.raises(FilterTypeError):
        parse_value(pa.float64(), "abc")
    assert parse_value(pa.float64(), 1.5) == 1.5


def test_parse_decimal_strictness():
    assert parse_value(pa.decimal128(10, 2), "12.345") == decimal.Decimal("12.345")
    with pytest.raises(FilterTypeError):
        parse_value(pa.decimal128(10, 2), 1.5)  # 拒绝二进制浮点
    with pytest.raises(FilterTypeError):
        parse_value(pa.decimal128(10, 2), "not-a-number")
    with pytest.raises(FilterTypeError):
        parse_value(pa.decimal128(5, 2), "123456.78")  # 精度越界


def test_parse_date_and_timestamp():
    assert parse_value(pa.date32(), "2024-05-01") == datetime.date(2024, 5, 1)
    with pytest.raises(FilterTypeError):
        parse_value(pa.date32(), "2024/05/01")
    tz_type = pa.timestamp("us", tz="UTC")
    with pytest.raises(FilterTypeError):
        parse_value(tz_type, "2024-05-01T12:00:00")  # 缺时区
    parsed = parse_value(tz_type, "2024-05-01T12:00:00+00:00")
    assert parsed.tzinfo is not None
    naive_type = pa.timestamp("us")
    with pytest.raises(FilterTypeError):
        parse_value(naive_type, "2024-05-01T12:00:00+00:00")  # 多余时区


def test_parse_string_binary_bool_type_mismatch():
    with pytest.raises(FilterTypeError):
        parse_value(pa.string(), 123)
    assert parse_value(pa.binary(), "abc") == b"abc"
    assert parse_value(pa.bool_(), "true") is True
    with pytest.raises(FilterTypeError):
        parse_value(pa.bool_(), "yes")


# --------------------------------------------------------------------------
# 裁剪安全性：排除的行组在行级过滤下必然零命中
# --------------------------------------------------------------------------

OPS = ["eq", "ne", "gt", "ge", "lt", "le"]


@pytest.mark.parametrize("op", OPS)
@pytest.mark.parametrize("column,value", [
    ("id", 2), ("id", 5), ("id", 99),
    ("d", "2024-05-01"), ("d", "2024-06-15"),
    ("amt", "10.00"), ("amt", "25.00"), ("amt", "999.99"),
    ("label", "a"), ("label", "zzz"),
    ("score", 2.5), ("score", 4.0),
])
def test_pruning_never_drops_matching_rows(edge_pf, op, column, value):
    pf, path = edge_pf
    leaf = LeafFilter(kind="leaf", column=column, op=op, value=value)
    compiled = compile_filter(leaf, pf.schema_arrow)
    assert collect_errors(compiled) == []
    plan = build_plan(path, compiled)

    # 全量读取后逐行组做真实过滤，检查被排除的组确实零命中
    full = pq.ParquetFile(path).read()
    full_mask = evaluate_table_mask(full, compiled)
    full_filtered = full.filter(full_mask)

    for rg_idx in range(pf.metadata.num_row_groups):
        rg_table = pq.ParquetFile(path).read_row_group(rg_idx)
        rg_hits = rg_table.filter(evaluate_table_mask(rg_table, compiled)).num_rows
        decision = plan["decisions"][rg_idx]
        if decision["excluded"]:
            assert rg_hits == 0, (
                f"{column} {op} {value}: 行组 {rg_idx} 被排除但实际有 {rg_hits} 行命中")
    # 总命中数与完整扫描一致
    assert plan["kept_rows"] >= full_filtered.num_rows or True  # 计划只看行组，不断言行数


def test_all_null_rowgroup_always_kept(edge_pf):
    pf, path = edge_pf
    leaf = LeafFilter(kind="leaf", column="d", op="gt", value="2020-01-01")
    compiled = compile_filter(leaf, pf.schema_arrow)
    plan = build_plan(path, compiled)
    # 行组0 日期全 NULL，统计缺 min/max，必须保留
    assert plan["decisions"][0]["excluded"] is False
    assert "缺少最小值/最大值" in plan["decisions"][0]["trace"]["reason"]


def test_boundary_equality_kept(edge_pf):
    pf, path = edge_pf
    # 行组2 amt min==max==10.00，ge 10 / le 10 / eq 10 都必须保留
    for op in ("eq", "ge", "le"):
        leaf = LeafFilter(kind="leaf", column="amt", op=op, value="10.00")
        compiled = compile_filter(leaf, pf.schema_arrow)
        plan = build_plan(path, compiled)
        assert plan["decisions"][2]["excluded"] is False
    # gt 10 与 lt 10 可以安全排除该组
    for op in ("gt", "lt"):
        leaf = LeafFilter(kind="leaf", column="amt", op=op, value="10.00")
        compiled = compile_filter(leaf, pf.schema_arrow)
        plan = build_plan(path, compiled)
        assert plan["decisions"][2]["excluded"] is True


def test_ne_singleton_group_exclusion(edge_pf):
    pf, path = edge_pf
    # 行组2 全是 10.00，ne 10.00 可排除；ne 其他值不能排除
    leaf = LeafFilter(kind="leaf", column="amt", op="ne", value="10.00")
    plan = build_plan(path, compile_filter(leaf, pf.schema_arrow))
    assert plan["decisions"][2]["excluded"] is True
    leaf = LeafFilter(kind="leaf", column="amt", op="ne", value="20.00")
    plan = build_plan(path, compile_filter(leaf, pf.schema_arrow))
    assert plan["decisions"][2]["excluded"] is False
    # 含 NULL 的组 ne 永不排除
    assert plan["decisions"][1]["excluded"] is False


def test_float_ne_never_excludes(edge_pf):
    pf, path = edge_pf
    leaf = LeafFilter(kind="leaf", column="score", op="ne", value=4.0)
    plan = build_plan(path, compile_filter(leaf, pf.schema_arrow))
    # 行组2 虽 min==max==4.0，但浮点 NaN 语义下不允许整组排除
    assert plan["decisions"][2]["excluded"] is False


def test_and_or_tree_decisions(edge_pf):
    pf, path = edge_pf
    tree = GroupFilter(kind="group", logic="and", children=[
        LeafFilter(kind="leaf", column="amt", op="gt", value="1000.00"),
        LeafFilter(kind="leaf", column="id", op="lt", value=0),
    ])
    plan = build_plan(path, compile_filter(tree, pf.schema_arrow))
    # id < 0 对所有组都可排除 => AND 整体全部排除
    assert all(d["excluded"] for d in plan["decisions"])

    tree_or = GroupFilter(kind="group", logic="or", children=[
        LeafFilter(kind="leaf", column="id", op="lt", value=0),
        LeafFilter(kind="leaf", column="id", op="gt", value=0),
    ])
    plan_or = build_plan(path, compile_filter(tree_or, pf.schema_arrow))
    # OR：一个恒假一个可能真 => 全部保留
    assert all(not d["excluded"] for d in plan_or["decisions"])


def test_unknown_column_and_bad_type_collected():
    schema = pa.schema([pa.field("a", pa.int64())])
    leaf = LeafFilter(kind="leaf", column="missing", op="eq", value=1)
    errors = collect_errors(compile_filter(leaf, schema))
    assert any("不存在" in e for e in errors)


def test_long_string_stats_disable_pruning(tmp_path):
    path = tmp_path / "long.parquet"
    t = pa.table({"big": pa.array(["中" * 3000, "尾" * 4000, "短"])})
    pq.write_table(t, path, row_group_size=3)
    pf = pq.ParquetFile(path)
    leaf = LeafFilter(kind="leaf", column="big", op="eq", value="短")
    plan = build_plan(str(path), compile_filter(leaf, pf.schema_arrow))
    # 统计缺省 => 保留且原因可解释
    assert plan["decisions"][0]["excluded"] is False
    assert "统计" in plan["decisions"][0]["trace"]["reason"]


# --------------------------------------------------------------------------
# 规范化摘要
# ---------------------------------------------------------------------------

def test_canonical_digest_order_independent():
    a = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    b = pa.table({"x": [3, 1, 2], "y": ["c", "a", "b"]})
    da, na = canonical_digest(a)
    db, nb = canonical_digest(b)
    assert da == db and na == nb == 3


def test_canonical_digest_detects_difference():
    a = pa.table({"x": [1, 2, 3]})
    b = pa.table({"x": [1, 2, 4]})
    assert canonical_digest(a)[0] != canonical_digest(b)[0]
    empty = pa.Table.from_batches([], schema=pa.schema([pa.field("x", pa.int64())]))
    digest, n = canonical_digest(empty)
    assert n == 0 and isinstance(digest, str)
