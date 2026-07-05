#!/usr/bin/env bash

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
MINISWEAGENT_CONFIG="$PROJECT_ROOT/../mini-swe-agent/src/minisweagent/config/benchmarks/swebench.yaml"

if [ ! -f "$MINISWEAGENT_CONFIG" ]; then
    echo "mini-swe-agent benchmark config not found at: $MINISWEAGENT_CONFIG" >&2
    exit 1
fi

MSWEA_COST_TRACKING='ignore_errors' mini-extra swebench \
    --subset verified \
    --split test \
    --model nebius/moonshotai/Kimi-K2.6 \
    --slice '0:3' \
    --config "$MINISWEAGENT_CONFIG" \
    --workers 5 \
    -o trajectories
