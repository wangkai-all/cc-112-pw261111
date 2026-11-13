# -*- coding: utf-8 -*-
"""筛选条件解析与行组裁剪判定。

设计原则（与需求逐条对应）：
1. 只有 Parquet 统计信息能够 *安全* 排除整组时才排除；任何缺失最小值/最大值、
   存在空值（对需要考虑 NULL 的语义）、边界相等、统计被截断的情况都保留。
2. 字符串按 Parquet 的 UTF-8 无符号字节序比较，数值按数值语义比较；
   输入值与列类型不匹配时抛出 FilterTypeError，而不是隐式转换。
3. 支持 AND / OR 组合，每个叶子与每个组都记录判定原因（中文），便于页面解释。
"""

from __future__ import annotations

import datetime
import decimal
import math
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Optional, Union

import pyarrow as pa
from pydantic import BaseModel, Field, model_validator

# ---------------------------------------------------------------------------
# 前端交互用的筛选树（Pydantic 模型，带判别联合的递归结构）
# ---------------------------------------------------------------------------

OPERATORS = ("eq", "ne", "gt", "ge", "lt", "le")


class LeafFilter(BaseModel):
    kind: str = Field("leaf", pattern="^leaf$")
    column: str
    op: str
    value: Any  # 数值/字符串，日期与时间戳统一用 ISO 字符串传递

    @model_validator(mode="after")
    def _check_op(self) -> "LeafFilter":
        if self.op not in OPERATORS:
            raise ValueError(f"不支持的比较运算符: {self.op!r}，可选 {OPERATORS}")
        if not isinstance(self.column, str) or not self.column:
            raise ValueError("筛选列名必须是非空字符串")
        return self


class GroupFilter(BaseModel):
    kind: str = Field("group", pattern="^group$")
    logic: str  # "and" | "or"
    children: list["FilterNode"]

    @model_validator(mode="after")
    def _check(self) -> "GroupFilter":
        if self.logic not in ("and", "or"):
            raise ValueError(f"组合逻辑只能是 and / or，收到: {self.logic!r}")
        if not self.children:
            raise ValueError("组合条件至少包含一个子条件")
        return self


FilterNode = Union[LeafFilter, GroupFilter]
GroupFilter.model_rebuild()


class FilterTypeError(Exception):
    """输入值无法按列的 Arrow 类型解释时抛出。"""


# ---------------------------------------------------------------------------
# Arrow 类型分类
# ---------------------------------------------------------------------------

def type_family(arrow_type: pa.DataType) -> str:
    if pa.types.is_dictionary(arrow_type):
        return type_family(arrow_type.value_type)
    if pa.types.is_integer(arrow_type):
        return "integer"
    if pa.types.is_floating(arrow_type):
        return "float"
    if pa.types.is_decimal(arrow_type):
        return "decimal"
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "string"
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return "binary"
    if pa.types.is_date(arrow_type):
        return "date"
    if pa.types.is_timestamp(arrow_type):
        return "timestamp"
    if pa.types.is_boolean(arrow_type):
        return "boolean"
    return "unsupported"


FAMILY_LABELS = {
    "integer": "整数",
    "float": "浮点数",
    "decimal": "定点小数",
    "string": "字符串",
    "binary": "二进制字节",
    "date": "日期",
    "timestamp": "时间戳",
    "boolean": "布尔",
    "unsupported": "不支持筛选的类型",
}

# 比较运算符的中文描述
OP_LABELS = {
    "eq": "=", "ne": "≠", "gt": ">", "ge": "≥", "lt": "<", "le": "≤",
}
OP_FULL_LABELS = {
    "eq": "等于", "ne": "不等于", "gt": "大于",
    "ge": "大于等于", "lt": "小于", "le": "小于等于",
}


# ---------------------------------------------------------------------------
# 输入值解析：严格按列类型解释，类型不符直接报错
# ---------------------------------------------------------------------------

def _parse_int(fam_type: pa.DataType, raw: Any) -> int:
    # bool 是 int 的子类，但整数列不接受布尔值
    if isinstance(raw, bool):
        raise FilterTypeError(f"整数列不接受布尔值 {raw!r}")
    if isinstance(raw, int):
        value = raw
    elif isinstance(raw, str):
        s = raw.strip()
        try:
            value = int(s, 10)
        except ValueError:
            raise FilterTypeError(f"值 {raw!r} 无法解析为整数")
    else:
        raise FilterTypeError(f"整数列只接受整数输入，收到 {type(raw).__name__}: {raw!r}")
    # 交给 Arrow 做范围检查（超界会抛 ArrowInvalid）
    try:
        pa.scalar(value, type=fam_type)
    except Exception as exc:  # noqa: BLE001 - 统一转换为业务错误
        raise FilterTypeError(f"值 {raw!r} 超出列类型 {fam_type} 的表示范围") from exc
    return value


def _parse_float(fam_type: pa.DataType, raw: Any) -> float:
    if isinstance(raw, bool):
        raise FilterTypeError(f"浮点列不接受布尔值 {raw!r}")
    if isinstance(raw, (int, float)):
        value = float(raw)
    elif isinstance(raw, str):
        s = raw.strip().lower()
        try:
            value = float(s)
        except ValueError:
            raise FilterTypeError(f"值 {raw!r} 无法解析为浮点数")
    else:
        raise FilterTypeError(f"浮点列只接受数值输入，收到 {type(raw).__name__}: {raw!r}")
    # NaN / Infinity 与统计信息的 min/max 无法安全配合，直接拒绝
    if not math.isfinite(value):
        raise FilterTypeError(f"浮点筛选值必须是有限数，收到 {raw!r}")
    return value


def _parse_decimal(fam_type: pa.DataType, raw: Any) -> Decimal:
    if isinstance(raw, bool):
        raise FilterTypeError(f"定点小数列不接受布尔值 {raw!r}")
    if isinstance(raw, Decimal):
        value = raw
    elif isinstance(raw, int):
        value = Decimal(raw)
    elif isinstance(raw, float):
        # 禁止隐式二进制浮点 -> 十进制，避免 0.1+0.2 之类的意外
        raise FilterTypeError(
            f"定点小数列不接受二进制浮点 {raw!r}，请改用十进制字符串，例如 '12.34'")
    elif isinstance(raw, str):
        s = raw.strip()
        try:
            value = Decimal(s)
        except decimal.InvalidOperation:
            raise FilterTypeError(f"值 {raw!r} 无法解析为定点小数")
        if not value.is_finite():
            raise FilterTypeError(f"定点小数筛选值必须有限，收到 {raw!r}")
    else:
        raise FilterTypeError(
            f"定点小数列只接受整数或十进制字符串，收到 {type(raw).__name__}: {raw!r}")
    # 精度/越界检查：扩到能容纳输入的 scale，再验证 Arrow 可建标量
    input_scale = max(0, -value.as_tuple().exponent)
    scale = max(fam_type.scale, input_scale)
    scaled_digits = value.scaleb(scale).as_tuple().digits
    int_digits = len(scaled_digits) - scale
    needed_precision = max(int_digits, 0) + scale
    # 列本身精度容纳不下该值（含扩 scale 后的位数要求）即报错
    if needed_precision > fam_type.precision:
        raise FilterTypeError(
            f"值 {raw!r} 需要精度 {needed_precision}，超过列类型 {fam_type} 的精度 "
            f"{fam_type.precision}")
    precision = fam_type.precision
    if pa.types.is_decimal128(fam_type) and precision > 38:
        raise FilterTypeError(f"值 {raw!r} 超过 Decimal128 的最大精度 38")
    if pa.types.is_decimal256(fam_type) and precision > 76:
        raise FilterTypeError(f"值 {raw!r} 超过 Decimal256 的最大精度 76")
    try:
        pa.scalar(value, type=pa.decimal128(precision, scale))
    except Exception as exc:  # noqa: BLE001
        raise FilterTypeError(f"值 {raw!r} 无法按 {fam_type} 表示（精度或越界）") from exc
    return value


def _parse_date(raw: Any) -> datetime.date:
    if isinstance(raw, datetime.datetime):
        return raw.date()
    if isinstance(raw, datetime.date):
        return raw
    if not isinstance(raw, str):
        raise FilterTypeError(f"日期列只接受 YYYY-MM-DD 字符串，收到 {type(raw).__name__}: {raw!r}")
    s = raw.strip()
    try:
        return datetime.date.fromisoformat(s)
    except ValueError:
        raise FilterTypeError(f"值 {raw!r} 不是合法日期（要求 YYYY-MM-DD）")


def _parse_timestamp(fam_type: pa.Timestamp, raw: Any) -> datetime.datetime:
    if isinstance(raw, datetime.datetime):
        dt = raw
    elif isinstance(raw, datetime.date) and not isinstance(raw, datetime.datetime):
        dt = datetime.datetime(raw.year, raw.month, raw.day)
    elif isinstance(raw, str):
        s = raw.strip().replace("Z", "+00:00")
        try:
            dt = datetime.datetime.fromisoformat(s)
        except ValueError:
            raise FilterTypeError(
                f"值 {raw!r} 不是合法时间戳（要求 ISO 8601，如 2024-05-01T12:00:00）")
    else:
        raise FilterTypeError(
            f"时间戳列只接受 ISO 8601 日期时间字符串，收到 {type(raw).__name__}: {raw!r}")

    tz = fam_type.tz
    if tz is not None:
        # 带时区的列：输入必须显式带时区，避免静默按服务器本地时区解释
        if dt.tzinfo is None:
            raise FilterTypeError(
                f"该时间戳列带时区 {tz}，筛选值也必须显式带时区偏移，"
                f"例如 '2024-05-01T12:00:00+00:00'")
        import zoneinfo
        try:
            target_tz = zoneinfo.ZoneInfo(tz)
        except Exception:  # noqa: BLE001
            target_tz = datetime.timezone.utc
        return dt.astimezone(target_tz)
    # 不带时区的列：拒绝带时区输入，避免隐式换算
    if dt.tzinfo is not None:
        raise FilterTypeError("该时间戳列不带时区，筛选值也不能带时区偏移")
    return dt


def _parse_string(raw: Any) -> str:
    if isinstance(raw, str):
        return raw
    raise FilterTypeError(f"字符串列只接受文本输入，收到 {type(raw).__name__}: {raw!r}")


def _parse_binary(raw: Any) -> bytes:
    if isinstance(raw, str):
        try:
            return raw.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise FilterTypeError(f"值 {raw!r} 无法编码为 UTF-8 字节") from exc
    raise FilterTypeError(f"二进制列只接受（可 UTF-8 编码的）文本输入，收到 {type(raw).__name__}")


def _parse_bool(raw: Any) -> bool:
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str) and raw.strip().lower() in ("true", "false"):
        return raw.strip().lower() == "true"
    raise FilterTypeError(f"布尔列只接受 true / false，收到 {raw!r}")


def parse_value(arrow_type: pa.DataType, raw: Any) -> Any:
    """按列类型把前端原始值解释为可与统计值比较的 Python 对象。"""
    fam = type_family(arrow_type)
    if fam == "integer":
        return _parse_int(arrow_type, raw)
    if fam == "float":
        return _parse_float(arrow_type, raw)
    if fam == "decimal":
        return _parse_decimal(arrow_type, raw)
    if fam == "date":
        return _parse_date(raw)
    if fam == "timestamp":
        return _parse_timestamp(arrow_type, raw)
    if fam == "string":
        return _parse_string(raw)
    if fam == "binary":
        return _parse_binary(raw)
    if fam == "boolean":
        return _parse_bool(raw)
    raise FilterTypeError(f"列类型 {arrow_type} 不支持筛选")


# ---------------------------------------------------------------------------
# 编译后的筛选树（解析一次，复用于所有行组与真实过滤）
# ---------------------------------------------------------------------------

@dataclass
class CompiledLeaf:
    column: str
    op: str
    raw_value: Any
    arrow_type: pa.DataType
    family: str
    value: Any
    errors: list[str] = field(default_factory=list)

    @property
    def valid(self) -> bool:
        return not self.errors


@dataclass
class CompiledGroup:
    logic: str
    children: list[Any]


def compile_filter(node: Any, schema: pa.Schema) -> Any:
    """递归校验并编译筛选树，收集全部类型错误（不遇错即停）。"""
    if isinstance(node, LeafFilter):
        errors: list[str] = []
        if node.column not in schema.names:
            errors.append(f"列 {node.column!r} 不存在于文件 schema 中")
            arrow_type = pa.null()
            family = "unsupported"
            value = None
        else:
            arrow_type = schema.field(node.column).type
            family = type_family(arrow_type)
            if family == "unsupported":
                errors.append(
                    f"列 {node.column!r} 的类型 {arrow_type} 不支持统计裁剪与筛选")
                value = None
            else:
                try:
                    value = parse_value(arrow_type, node.value)
                except FilterTypeError as exc:
                    errors.append(str(exc))
                    value = None
        return CompiledLeaf(
            column=node.column, op=node.op, raw_value=node.value,
            arrow_type=arrow_type, family=family, value=value, errors=errors)

    children = [compile_filter(ch, schema) for ch in node.children]
    return CompiledGroup(logic=node.logic, children=children)


def collect_errors(compiled: Any) -> list[str]:
    if isinstance(compiled, CompiledLeaf):
        return list(compiled.errors)
    out: list[str] = []
    for ch in compiled.children:
        out.extend(collect_errors(ch))
    return out


def describe_leaf(leaf: CompiledLeaf) -> str:
    shown = leaf.raw_value
    if isinstance(shown, str) and len(shown) > 40:
        shown = shown[:37] + "..."
    return f"{leaf.column} {OP_FULL_LABELS[leaf.op]} {shown!r}"


# ---------------------------------------------------------------------------
# 行组级裁剪判定
#
# 返回 (excluded: bool, reason: str)。excluded=True 表示可以安全跳过整组。
# ---------------------------------------------------------------------------

# Parquet 字符串统计在值过长时可能被截断或直接缺省；pyarrow 在超过约 1KB 时
# 会省略 min/max。对仍保留下来的长值无法从 Python 侧确认是否被截断，
# 因此设定保守阈值：超过阈值的字符串/二进制统计一律视为不可裁剪依据。
MAX_TRUSTED_STAT_BYTES = 1024


def _stat_unavailable_reason(stats: Any, leaf: CompiledLeaf) -> Optional[str]:
    """统计无法作为裁剪依据时返回原因，否则返回 None。"""
    if stats is None:
        return "该列没有统计信息块"
    if not stats.has_min_max:
        return "缺少最小值/最大值统计（可能整组为空或写入时未生成）"
    if leaf.family in ("string", "binary"):
        mn, mx = stats.min, stats.max
        try:
            mn_b = mn.encode("utf-8") if isinstance(mn, str) else bytes(mn)
            mx_b = mx.encode("utf-8") if isinstance(mx, str) else bytes(mx)
        except Exception:  # noqa: BLE001
            return "字符串统计无法按 UTF-8 解释"
        if len(mn_b) > MAX_TRUSTED_STAT_BYTES or len(mx_b) > MAX_TRUSTED_STAT_BYTES:
            return ("最小/最大值长度超过安全阈值 "
                    f"{MAX_TRUSTED_STAT_BYTES} 字节，统计可能已被截断，不能据此裁剪")
    return None


def _values_equal(a: Any, b: Any, family: str) -> bool:
    if family == "decimal":
        return Decimal(a) == Decimal(b)
    return a == b


def evaluate_leaf_on_rowgroup(leaf: CompiledLeaf, stats: Any) -> tuple[bool, str]:
    """对单个行组的单列统计做判定。"""
    if not leaf.valid:
        return False, "筛选条件存在类型错误，保守保留该行组：" + "；".join(leaf.errors)

    missing = _stat_unavailable_reason(stats, leaf)
    if missing is not None:
        return False, f"保留：{missing}，无法保证安全裁剪"

    mn, mx = stats.min, stats.max
    op, v, fam = leaf.op, leaf.value, leaf.family
    nulls = stats.null_count or 0
    col = leaf.column

    # 定点小数统一走 Decimal 比较（允许不同 scale）
    if fam == "decimal":
        mn_d, mx_d, v_d = Decimal(mn), Decimal(mx), Decimal(v)
        cmp_lo, cmp_hi, cmp_v = mn_d, mx_d, v_d
    else:
        cmp_lo, cmp_hi, cmp_v = mn, mx, v

    point = f"统计区间 [{mn!r}, {mx!r}]"

    if op == "eq":
        if cmp_v < cmp_lo or cmp_v > cmp_hi:
            return True, f"排除：目标值 {v!r} 落在 {point} 之外，整组不可能出现该值"
        if nulls > 0:
            return False, (f"保留：{v!r} 位于 {point} 内，且该组有 {nulls} 个空值"
                           "（等值查询不匹配 NULL，但区间命中已足以要求读取）")
        return False, f"保留：{v!r} 位于 {point} 内（含边界相等情形），需要读取确认"

    if op == "ne":
        # 只有“整组非空且唯一值就等于 v”时才能排除。
        # 浮点不做此优化（NaN 不参与 min/max，无法证明组内没有其他值）。
        if fam == "float":
            return False, "保留：浮点列存在 NaN 不进入 min/max 的可能，不等于查询不做整组排除"
        if nulls > 0:
            return False, f"保留：该组含 {nulls} 个 NULL，而 NULL ≠ {v!r} 的比较结果为 NULL（不满足筛选），但不能跳过"
        if _values_equal(mn, mx, fam) and _values_equal(mn, v, fam):
            return True, f"排除：整组非空且唯一值就是 {v!r}，{OP_FULL_LABELS['ne']} 条件无一行满足"
        return False, f"保留：{point} 内存在不等于 {v!r} 的可能（含边界相等情形）"

    if op == "gt":  # 列 > v：mx <= v 时整组无满足
        if cmp_hi <= cmp_v:
            if cmp_hi == cmp_v:
                return True, (f"排除：最大值 {mx!r} 与边界 {v!r} 恰好相等，"
                              "严格大于(>)在边界上不成立，且最小值不超过最大值")
            return True, f"排除：最大值 {mx!r} ≤ 边界 {v!r}，整组没有大于边界的值"
        return False, f"保留：最大值 {mx!r} > 边界 {v!r}，组内可能存在满足条件的行"

    if op == "ge":  # 列 >= v：mx < v 时整组无满足
        if cmp_hi < cmp_v:
            return True, f"排除：最大值 {mx!r} < 边界 {v!r}，整组没有满足 ≥ 条件的值"
        if cmp_hi == cmp_v:
            return False, (f"保留：最大值 {mx!r} 与边界 {v!r} 恰好相等，"
                           "≥ 在边界上成立，必须读取以保留边界行")
        return False, f"保留：最大值 {mx!r} ≥ 边界 {v!r}，组内可能存在满足条件的行"

    if op == "lt":  # 列 < v：mn >= v 时整组无满足
        if cmp_lo >= cmp_v:
            if cmp_lo == cmp_v:
                return True, (f"排除：最小值 {mn!r} 与边界 {v!r} 恰好相等，"
                              "严格小于(<)在边界上不成立")
            return True, f"排除：最小值 {mn!r} ≥ 边界 {v!r}，整组没有小于边界的值"
        return False, f"保留：最小值 {mn!r} < 边界 {v!r}，组内可能存在满足条件的行"

    if op == "le":  # 列 <= v：mn > v 时整组无满足
        if cmp_lo > cmp_v:
            return True, f"排除：最小值 {mn!r} > 边界 {v!r}，整组没有满足 ≤ 条件的值"
        if cmp_lo == cmp_v:
            return False, (f"保留：最小值 {mn!r} 与边界 {v!r} 恰好相等，"
                           "≤ 在边界上成立，必须读取以保留边界行")
        return False, f"保留：最小值 {mn!r} ≤ 边界 {v!r}，组内可能存在满足条件的行"

    return False, f"保留：未识别的运算符 {op}，保守处理"  # pragma: no cover


def evaluate_rowgroup(compiled: Any, row_group_stats: dict[str, Any]) -> tuple[bool, dict]:
    """对整棵筛选树判定一个行组。

    row_group_stats: {列名: Parquet ColumnChunkStatistics 或 None}
    返回 (excluded, trace)。trace 为可 JSON 序列化的判定说明。
    """
    if isinstance(compiled, CompiledLeaf):
        stats = row_group_stats.get(compiled.column)
        excluded, reason = evaluate_leaf_on_rowgroup(compiled, stats)
        return excluded, {
            "kind": "leaf",
            "column": compiled.column,
            "op": compiled.op,
            "op_label": OP_FULL_LABELS[compiled.op],
            "value": _json_safe(compiled.raw_value),
            "family": compiled.family,
            "family_label": FAMILY_LABELS.get(compiled.family, compiled.family),
            "valid": compiled.valid,
            "errors": compiled.errors,
            "excluded": excluded,
            "decision": "排除" if excluded else "保留",
            "reason": reason,
        }

    child_results = [evaluate_rowgroup(ch, row_group_stats) for ch in compiled.children]
    child_excluded = [r[0] for r in child_results]
    child_traces = [r[1] for r in child_results]

    if compiled.logic == "and":
        # AND：任一叶子可安全排除 => 整组排除（合取中一个恒假即可跳过）
        excluded = any(child_excluded)
        if excluded:
            names = [t["reason"] for t, ex in zip(child_traces, child_excluded) if ex]
            reason = "排除（AND）：至少一个子条件可安全排除整组 → " + "；".join(names)
        else:
            reason = "保留（AND）：所有子条件都无法安全排除整组"
    else:
        # OR：必须所有叶子都能安全排除 => 才可排除（析取中全部恒假才能跳过）
        excluded = all(child_excluded)
        if excluded:
            reason = "排除（OR）：每个子条件都证明整组无匹配，合取后仍可安全跳过"
        else:
            reason = "保留（OR）：至少一个子条件仍可能匹配，不能跳过整组"

    return excluded, {
        "kind": "group",
        "logic": compiled.logic,
        "excluded": excluded,
        "decision": "排除" if excluded else "保留",
        "reason": reason,
        "children": child_traces,
    }


def _json_safe(v: Any) -> Any:
    if isinstance(v, (datetime.datetime, datetime.date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    return v
