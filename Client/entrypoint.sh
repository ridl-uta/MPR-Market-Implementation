#!/bin/bash
set -euo pipefail

# Defaults (overridable via env vars)
: "${JOB_CHOICES:=xsbench minife comd}"
: "${DELAY_START:=200}"
: "${DELAY_STEP:=10}"

SHARED_DIR="/shared"
COUNTER_FILE="$SHARED_DIR/seq.txt"
LOCK_DIR="$SHARED_DIR/.lock"

mkdir -p "$SHARED_DIR"

pick_job() {
  # If caller passed a job explicitly, honor it
  if [[ -n "${1-}" ]]; then
    echo "$1"
    return
  fi

  # Otherwise pick randomly from JOB_CHOICES
  read -r -a choices <<< "$JOB_CHOICES"
  if [[ ${#choices[@]} -eq 0 ]]; then
    echo "xsbench"
    return
  fi
  echo "${choices[$((RANDOM % ${#choices[@]}))]}"
}

next_index() {
  # Cross-container counter backed by shared volume
  while ! mkdir "$LOCK_DIR" 2>/dev/null; do
    sleep 0.05
  done

  if [[ -f "$COUNTER_FILE" ]]; then
    idx=$(cat "$COUNTER_FILE" || echo 0)
  else
    idx=0
  fi

  echo $((idx + 1)) > "$COUNTER_FILE"
  rmdir "$LOCK_DIR" || true

  echo "$idx"
}

JOB_NAME=$(pick_job "${1-}")

# Compute incremental delay using env vars (DELAY_START + n*DELAY_STEP)
idx=$(next_index)
DELAY_SEC=$((DELAY_START + (idx * DELAY_STEP)))

echo "[Entrypoint] Selected job: $JOB_NAME"
echo "[Entrypoint] Sleeping for $DELAY_SEC seconds before starting..."
sleep "$DELAY_SEC"

exec python3 main.py \
  --job "$JOB_NAME" \
  --perf_data_path /data/all_model_data.xlsx \
  --host server \
  --port 8000 \
  --http_port 5000
