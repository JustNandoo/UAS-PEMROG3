#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

PYTHON_BIN="${PYTHON_BIN:-}"
if [ -z "$PYTHON_BIN" ]; then
  for candidate in python3.12 python3.11 python3.10; do
    if command -v "$candidate" >/dev/null 2>&1; then
      PYTHON_BIN="$candidate"
      break
    fi
  done
fi

if [ -z "$PYTHON_BIN" ]; then
  echo "Python 3.10-3.12 tidak ditemukan. Pasang salah satunya atau set PYTHON_BIN=/path/ke/python." >&2
  exit 1
fi

AIRFLOW_VERSION="$(grep -E '^apache-airflow==' requirements.txt | cut -d= -f3)"
PY_VERSION="$("$PYTHON_BIN" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"

"$PYTHON_BIN" -m venv .venv
.venv/bin/pip install --quiet -r requirements.txt \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY_VERSION}.txt"

echo "Setup selesai (Python ${PY_VERSION}, Airflow ${AIRFLOW_VERSION}). Jalankan: ./scripts/run_airflow.sh"
