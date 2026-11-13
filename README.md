# Parquet 行组裁剪实验台

上传 Parquet 后在页面查看每个行组的行数、列类型与 min/max/NULL 统计；构造日期、
金额等筛选条件，先预览哪些行组可以被**安全排除**、哪些仍需读取，再执行查询查看
真实结果。内置同一数据集按不同排序与行组大小写出的多个变体，用于对比候选行组、
实际磁盘读取量与结果一致性，并提供关闭裁剪的完整扫描作为对照。

## 快速开始

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python -I scripts/generate_samples.py   # 生成内置样本（data/samples/）
./run.sh                                           # http://127.0.0.1:8077
```

测试：

```bash
.venv/bin/python -m pytest tests/ -q
```

## 裁剪的安全语义（核心约束）

裁剪只使用能够**保证不漏记录**的统计条件，规则实现于 `app/filters.py`：

1. **缺少最小值/最大值**（统计块缺失、整组全 NULL、写入时未生成）→ 一律保留。
2. **长字符串/二进制统计可能被截断或直接缺省**（Parquet 对超长值不写 min/max）
   → 无法确认边界时一律保留，代码内置 1024 字节保守阈值。
3. **筛选边界与统计值恰好相等**时按比较运算符的真实语义处理：
   - `>` / `<`：边界相等不成立，可以排除（如 `max == v` 时 `列 > v` 整组无命中）；
   - `≥` / `≤` / `=`：边界相等成立，必须保留并读取确认；
   - `≠`：仅当整组非空且唯一值就等于该值时才能排除；浮点列因 NaN 不进入
     min/max，永不做整组排除。
4. **NULL 三值逻辑**：任何比较与 NULL 的结果都是 UNKNOWN，不满足筛选；含 NULL
   的行组在 `≠` 等场景下不能跳过。行级过滤用 `fill_null(mask, False)` 实现。
5. **类型严格匹配**：字符串按 Parquet 的 UTF-8 无符号字节序比较，数值按数值
   语义比较（定点小数统一 `Decimal` 比较，允许不同 scale）；输入值与列类型不
   匹配时返回 400 并列出全部错误，不做隐式转换。浮点筛选拒绝 NaN/Infinity；
   带时区时间戳要求输入显式带时区，不带时区列拒绝带时区输入。
6. **AND/OR 组合**：AND 组中任一叶子可安全排除整组即排除；OR 组必须所有叶子
   都能安全排除才排除。每个叶子和组都返回中文判定原因，页面逐层展示。

## 读取统计的两个口径（绝不拿文件总大小冒充读取量）

实现于 `app/parquet_store.py`：

- **计划读取量 `planned_data_bytes`**：明确的读取计划，等于候选行组中被选中
  列块的 `total_compressed_size` 之和，纯 footer 元数据即可算出。
- **实测读取量 `measured_data_bytes` / `measured_footer_bytes`**：执行记录。
  `CountingRawIO` 包装裸文件描述符，对每次 `read()` 返回的真实字节计数；
  footer（Parquet 打开时的尾部读取）与列块数据分别汇总，逐行组给出计量。
  测试验证了每个行组的实测字节恰好等于其计划字节。

## 结果一致性

结果摘要 `result_digest` 把结果表按全部列排序（NULL 置末）后做规范化 JSON 的
SHA-256，与行顺序无关。对比接口 `/api/compare` 对每个变体同时跑裁剪查询和完整
扫描，校验：跨变体摘要一致、且每个文件的裁剪结果与完整扫描完全相同。

## 内置样本

`scripts/generate_samples.py` 生成 12000 行订单数据的 5 个变体（未排序/按日期
排序/按金额排序 × 行组 2000/500），以及两个边界文件：

- `edge_cases_rg3.parquet`：全 NULL 行组、含 NaN/NULL 行组、单值边界行组；
- `long_strings.parquet`：超过统计安全长度、min/max 被省略的长字符串列。

## 目录结构

```
app/
  filters.py        # 筛选树解析、类型校验、行组裁剪判定与原因解释
  parquet_store.py  # 元数据、裁剪计划、真实读取计量、行级过滤、规范化摘要
  compare.py        # 多变体对比 API
  main.py           # FastAPI 路由
  static/           # 原生 JS 单页前端（无构建步骤）
scripts/generate_samples.py
tests/              # 97 个 pytest 用例（含“被排除行组必然零命中”不变量）
```

## 主要 API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/samples` | 内置样本列表 |
| POST | `/api/files/upload` | 上传 .parquet（独立目录、文件名净化、可解析才注册） |
| POST | `/api/files/register-sample/{name}` | 注册内置样本 |
| GET | `/api/files/{id}/metadata` | 行组、列类型、逐列统计 |
| POST | `/api/files/{id}/plan` | 裁剪计划预览（不读数据，含逐行组判定原因） |
| POST | `/api/files/{id}/query` | 执行查询（`pruning:true/false`） |
| POST | `/api/compare` | 多文件裁剪 vs 完整扫描对比 |
| GET | `/api/files/{id}/runs` | 历史执行记录 |
