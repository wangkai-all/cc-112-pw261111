#!/usr/bin/env bash
# 启动 Parquet 行组裁剪实验台
# 开发时如需代码改动自动重载，可先 pip install watchfiles，再追加 --reload
set -euo pipefail
cd "$(dirname "$0")"
exec .venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8077
