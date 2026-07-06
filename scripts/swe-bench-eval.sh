#!/usr/bin/env bash

set -euo pipefail

DATASET_NAME=${1:-princeton-nlp/SWE-bench_Verified}
PREDICTIONS_PATH=${2:-trajectories/preds.json}
MAX_WORKERS=${3:-5}
RUN_ID=${4:-test}

python -m swebench.harness.run_evaluation \
    --dataset_name "$DATASET_NAME" \
    --predictions_path "$PREDICTIONS_PATH" \
    --max_workers "$MAX_WORKERS" \
    --run_id "$RUN_ID"
