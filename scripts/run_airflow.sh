#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [ ! -x .venv/bin/airflow ]; then
  echo "Airflow belum terpasang. Jalankan dulu: ./scripts/setup.sh" >&2
  exit 1
fi

export AIRFLOW_HOME="$ROOT/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$ROOT/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export PATH="$ROOT/.venv/bin:$PATH"

if [ "$(uname)" = "Darwin" ]; then
  export PYTHONPATH="$ROOT/scripts/macos_fix${PYTHONPATH:+:$PYTHONPATH}"
  export NO_PROXY="*"
  export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
fi

airflow db migrate
airflow users create \
  --username "${AIRFLOW_ADMIN_USER:-admin}" \
  --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com >/dev/null 2>&1 || true

airflow scheduler &
airflow triggerer &
trap 'kill $(jobs -p) 2>/dev/null' EXIT

airflow webserver --port 8080
