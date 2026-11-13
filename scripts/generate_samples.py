# -*- coding: utf-8 -*-
"""生成同一数据集的多种 Parquet 变体（不同排序、不同行组大小）与边界用例文件。

所有变体由同一份内存表写出，保证“同一份数据”，用于比较：
  - 候选行组数量与计划读取量
  - 真实磁盘读取量（执行计量）
  - 结果一致性（规范化摘要 + 行数）
"""

from __future__ import annotations

import datetime
import decimal
import math
import random
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

SAMPLE_DIR = Path(__file__).resolve().parent.parent / "data" / "samples"
SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

REGIONS = ["华北", "华东", "华南", "西南", "东北", "海外"]
LEVELS = ["普通", "银卡", "金卡", "钻石"]
N = 12000
SEED = 20261114


def build_base_table() -> pa.Table:
    rnd = random.Random(SEED)
    ids, dates, amounts, levels, regions, notes, discounts = [], [], [], [], [], [], []
    start = datetime.date(2024, 1, 1)
    for i in range(N):
        ids.append(100000 + i)
        dates.append(start + datetime.timedelta(days=rnd.randrange(366)))
        amounts.append(decimal.Decimal(rnd.randrange(990, 888800)) / decimal.Decimal(100))
        levels.append(rnd.choice(LEVELS))
        regions.append(rnd.choice(REGIONS))
        notes.append(None if i % 9 == 0 else f"备注-{rnd.randrange(100000):05d}")
        d = round(rnd.uniform(0.0, 0.35), 4)
        discounts.append(None if i % 13 == 0 else d)
    return pa.table({
        "order_id": pa.array(ids, type=pa.int64()),
        "order_date": pa.array(dates, type=pa.date32()),
        "amount": pa.array(amounts, type=pa.decimal128(10, 2)),
        "level": pa.array(levels, type=pa.string()),
        "region": pa.array(regions, type=pa.string()),
        "note": pa.array(notes, type=pa.string()),
        "discount": pa.array(discounts, type=pa.float64()),
    })


def sort_table(table: pa.Table, keys: list[tuple[str, str]]) -> pa.Table:
    idx = pc.sort_indices(table, sort_keys=keys, null_placement="at_end")
    return table.take(idx)


def write_variant(table: pa.Table, name: str, row_group_size: int) -> Path:
    path = SAMPLE_DIR / name
    pq.write_table(
        table, path, row_group_size=row_group_size,
        compression="snappy", use_dictionary=True, write_statistics=True)
    return path


def main() -> None:
    base = build_base_table()

    variants = [
        ("orders_unsorted_rg2000.parquet", base, 2000),
        ("orders_unsorted_rg500.parquet", base, 500),
        ("orders_sorted_date_rg2000.parquet",
         sort_table(base, [("order_date", "ascending"), ("order_id", "ascending")]), 2000),
        ("orders_sorted_amount_rg2000.parquet",
         sort_table(base, [("amount", "ascending"), ("order_id", "ascending")]), 2000),
        ("orders_sorted_date_rg500.parquet",
         sort_table(base, [("order_date", "ascending"), ("order_id", "ascending")]), 500),
    ]
    for name, table, rg in variants:
        path = write_variant(table, name, rg)
        md = pq.ParquetFile(path).metadata
        print(f"{name}: {md.num_rows} 行, {md.num_row_groups} 行组, {path.stat().st_size} 字节")

    write_edge_cases()


def write_edge_cases() -> None:
    """覆盖：整组全 NULL、边界恰好相等、长字符串统计缺省、NaN、空组语义。"""
    # 行组0：order_date 全 NULL；行组1：有 NULL 也有值
    tables = [
        pa.table({
            "order_id": pa.array([1, 2, 3], type=pa.int64()),
            "order_date": pa.array([None, None, None], type=pa.date32()),
            "amount": pa.array([decimal.Decimal("10.00")] * 3, type=pa.decimal128(10, 2)),
            "label": pa.array(["x", "y", "z"], type=pa.string()),
            "score": pa.array([1.0, 2.0, 3.0], type=pa.float64()),
        }),
        pa.table({
            "order_id": pa.array([4, 5, 6], type=pa.int64()),
            "order_date": pa.array(
                [datetime.date(2024, 5, 1), None, datetime.date(2024, 5, 2)],
                type=pa.date32()),
            "amount": pa.array(
                [decimal.Decimal("10.00"), decimal.Decimal("20.00"), decimal.Decimal("30.00")],
                type=pa.decimal128(10, 2)),
            "label": pa.array(["a", None, "b"], type=pa.string()),
            "score": pa.array([float("nan"), 2.5, None], type=pa.float64()),
        }),
        # 行组2：单值边界（min == max == 10.00），用于 eq/ne 边界测试
        pa.table({
            "order_id": pa.array([7, 8], type=pa.int64()),
            "order_date": pa.array(
                [datetime.date(2024, 6, 1), datetime.date(2024, 6, 2)], type=pa.date32()),
            "amount": pa.array(
                [decimal.Decimal("10.00"), decimal.Decimal("10.00")], type=pa.decimal128(10, 2)),
            "label": pa.array(["短", "短"], type=pa.string()),
            "score": pa.array([4.0, 4.0], type=pa.float64()),
        }),
    ]
    edge = pa.concat_tables(tables)
    path = SAMPLE_DIR / "edge_cases_rg3.parquet"
    pq.write_table(edge, path, row_group_size=3, compression="snappy", write_statistics=True)

    # 超长字符串：超过 Parquet 统计安全长度后 min/max 缺省
    long_vals = ["中" * 3000, "尾" * 4000, "短"]
    long_table = pa.table({
        "order_id": pa.array([1, 2, 3], type=pa.int64()),
        "big_text": pa.array(long_vals, type=pa.string()),
    })
    long_path = SAMPLE_DIR / "long_strings.parquet"
    pq.write_table(long_table, long_path, row_group_size=3, compression="snappy")
    print(f"edge_cases_rg3.parquet 与 long_strings.parquet 已写出")


if __name__ == "__main__":
    main()
