#!/usr/bin/env python
"""Quick test of helpers without running the full pipeline."""
import shutil

from src.pipeline.helpers import (
    PROJECT_ROOT,
    build_run_config,
    collect_metrics,
    generate_run_id,
    prepare_run_dir,
)

# Test 1: Build config
params = {
    "split": "test",
    "subset": "verified",
    "workers": 2,
    "model": "test-model",
}
config = build_run_config(params)
print("✓ Config built:", config)

# Test 2: Generate run ID
run_id = generate_run_id(config)
print("✓ Run ID generated:", run_id)

# Test 3: Prepare run dir
run_dir = prepare_run_dir(run_id, config)
print("✓ Run directory created:", run_dir)

# Check structure
assert (run_dir / "config.json").exists()
assert (run_dir / "run-agent" / "trajectories").exists()
assert (run_dir / "run-eval" / "logs").exists()
assert (run_dir / "run-eval" / "reports").exists()
print("✓ All directories and files present")

# Test 4: Parse sample evaluation summary
sample_eval_dir = PROJECT_ROOT / "sample"
metrics = collect_metrics(sample_eval_dir)
assert metrics["total"] == 500
assert metrics["completed"] == 3
assert metrics["resolved"] == 1
print("✓ Sample metrics parsed:", metrics)

shutil.rmtree(run_dir)
print("✓ Temporary test run cleaned up")

print("\n✓ Phase 1 helpers validated!")

