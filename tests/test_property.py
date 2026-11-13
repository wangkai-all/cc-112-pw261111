# -*- coding: utf-8 -*-
"""随机化属性测试：任何被裁剪判定排除的行组，行级真实过滤必然零命中。

跨多种列类型、运算符、组合逻辑与随机数据反复验证，是整个工具安全性的总闸门。
"""

import datetime
import decimal
import random

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from app.filters import (
    GroupFilter,
    LeafFilter,
    collect_errors,
    compile_filter,
)
from app.parquet_store import build_plan, evaluate_table_mask

OPS = ["eq", "ne", "gt", "ge", "lt", "le"]


def _make_random_table(rng: random.Random, n: int) -> pa.Table:
    start = datetime.date(2024, 1, 1)
    return pa.table({
        "i": pa.array([rng.randint(-1000, 1000) if rng.random() > 0.1 else None
                       for _ in range(n)], type=pa.int64()),
        "f": pa.array([round(rng.uniform(-500, 500), 3) if rng.random() > 0.1 else None
                       for _ in range(n)], type=pa.float64()),
        "d": pa.array([start + datetime.timedelta(days=rng.randrange(365))
                       if rng.random() > 0.1 else None for _ in range(n)],
                      type=pa.date32()),
        "dec": pa.array(
            [decimal.Decimal(rng.randint(-99999, 99999)) / decimal.Decimal(100)
             if rng.random() > 0.1 else None for _ in range(n)],
            type=pa.decimal128(10, 2)),
        "s": pa.array([rng.choice(["alpha", "beta", "gamma", "delta", "ε"])
                       if rng.random() > 0.1 else None for _ in range(n)],
                      type=pa.string()),
    })


def _random_leaf(rng: random.Random, schema: pa.Schema):
    col = rng.choice([f.name for f in schema])
    op = rng.choice(OPS)
    fam_col = schema.field(col)
    import pyarrow.types as pt
    if pt.is_integer(fam_col.type):
        value = rng.randint(-1200, 1200)
    elif pt.is_floating(fam_col.type):
        value = round(rng.uniform(-600, 600), 2)  # 有限数
    elif pt.is_date(fam_col.type):
        value = (datetime.date(2024, 1, 1)
                 + datetime.timedelta(days=rng.randrange(-30, 400))).isoformat()
    elif pt.is_decimal(fam_col.type):
        value = f"{rng.randint(-120000, 120000) / 100:.2f}"
    else:
        value = rng.choice(["alpha", "beta", "gamma", "omega", "ε", "zzz"])
    return LeafFilter(kind="leaf", column=col, op=op, value=value)


def _random_tree(rng: random.Random, schema: pa.Schema, depth: int = 0):
    if depth >= 2 or rng.random() < 0.45:
        return _random_leaf(rng, schema)
    return GroupFilter(
        kind="group",
        logic=rng.choice(["and", "or"]),
        children=[_random_tree(rng, schema, depth + 1) for _ in
                  range(rng.randint(2, 3))])


@pytest.mark.parametrize("seed", range(40))
def test_random_pruning_safety(tmp_path, seed):
    rng = random.Random(seed)
    table = _make_random_table(rng, 1500)
    path = tmp_path / f"rand_{seed}.parquet"
    # 故意用小行组制造更多统计边界
    pq.write_table(table, path, row_group_size=rng.choice([128, 256, 500]))
    pf = pq.ParquetFile(path)

    tree = _random_tree(rng, pf.schema_arrow)
    compiled = compile_filter(tree, pf.schema_arrow)
    if collect_errors(compiled):
        pytest.skip("随机生成了类型不合法条件（解析器会拒绝）")

    plan = build_plan(str(path), compiled)

    # 不变量 1：每个被排除的行组，行级过滤零命中
    for rg_idx in range(pf.metadata.num_row_groups):
        rg_table = pf.read_row_group(rg_idx)
        hits = rg_table.filter(evaluate_table_mask(rg_table, compiled)).num_rows
        if plan["decisions"][rg_idx]["excluded"]:
            assert hits == 0, (
                f"seed={seed} 行组 {rg_idx} 被排除但有 {hits} 行命中: {tree.model_dump()}")

    # 不变量 2：裁剪读取 + 行级过滤的结果，与完整扫描的结果完全一致
    kept = pa.concat_tables([pf.read_row_group(i) for i in plan["kept_row_groups"]]) \
        if plan["kept_row_groups"] else pa.Table.from_batches([], schema=pf.schema_arrow)
    pruned_result = kept.filter(evaluate_table_mask(kept, compiled))
    full_result = table.filter(evaluate_table_mask(table, compiled))
    assert pruned_result.num_rows == full_result.num_rows
    # 内容一致（顺序可能不同，按列多重集比较）
    import pyarrow.compute as pc
    for col in table.column_names:
        a = pc.sort_indices(pruned_result, sort_keys=[(col, "ascending")],
                            null_placement="at_end")
        b = pc.sort_indices(full_result, sort_keys=[(col, "ascending")],
                            null_placement="at_end")
        assert pruned_result.column(col).take(a).to_pylist() == \
            full_result.column(col).take(b).to_pylist(), (seed, col)
