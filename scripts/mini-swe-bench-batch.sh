#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)

SUBSET=${1:-verified}
SPLIT=${2:-test}
MODEL=${3:-nebius/moonshotai/Kimi-K2.6}
TASK_SLICE=${4:-}
WORKERS=${5:-5}
OUTPUT_DIR=${6:-trajectories}

MINISWEAGENT_CONFIG=${MINISWEAGENT_BENCHMARK_CONFIG:-$PROJECT_ROOT/../mini-swe-agent/src/minisweagent/config/benchmarks/swebench.yaml}

if [ ! -f "$MINISWEAGENT_CONFIG" ]; then
    echo "mini-swe-agent benchmark config not found at: $MINISWEAGENT_CONFIG" >&2
    exit 1
fi

CMD=(
    mini-extra swebench
    --subset "$SUBSET"
    --split "$SPLIT"
    --model "$MODEL"
    --config "$MINISWEAGENT_CONFIG"
    --workers "$WORKERS"
    -o "$OUTPUT_DIR"
)

if [ -n "$TASK_SLICE" ]; then
    CMD+=(--slice "$TASK_SLICE")
fi

MSWEA_COST_TRACKING='ignore_errors' "${CMD[@]}"
