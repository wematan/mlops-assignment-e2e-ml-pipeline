import json
import os
import hashlib
import shutil
import subprocess
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUNS_ROOT = PROJECT_ROOT / "runs"


def _normalize_optional(value):
    if value in (None, "", "None", "null"):
        return None
    return value


def _dataset_name_for_subset(subset):
    subset_key = str(subset).strip().lower()
    if subset_key == "verified":
        return "princeton-nlp/SWE-bench_Verified"
    if subset_key == "lite":
        return "princeton-nlp/SWE-bench_Lite"
    return "princeton-nlp/SWE-bench"


def _find_eval_summary(eval_dir):
    for path in sorted(eval_dir.glob("*.json")):
        return path
    return None


def build_run_config(params):
    """Build config dict from Airflow params."""
    config = {
        "split": str(params.get("split", "test")),
        "subset": str(params.get("subset", "verified")),
        "workers": int(params.get("workers", 4)),
        "model": str(params.get("model", "nebius/moonshotai/Kimi-K2.6")),
        "task_slice": _normalize_optional(params.get("task_slice")),
        "cost_limit": str(params.get("cost_limit", "0")),
        "dataset_name": _dataset_name_for_subset(params.get("subset", "verified")),
        "timestamp": datetime.now().isoformat(),
    }
    return config


def generate_run_id(config):
    """Generate unique run ID from model and timestamp."""
    model_short = config["model"].replace("/", "__")[:20]
    ts = datetime.fromisoformat(config["timestamp"]).strftime("%Y%m%d_%H%M%S")
    model_hash = hashlib.md5(model_short.encode()).hexdigest()[:8]
    return f"{ts}_{model_short}_{model_hash}"


def prepare_run_dir(run_id, config):
    """Create run directory and save config."""
    run_dir = RUNS_ROOT / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    config_path = run_dir / "config.json"
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)

    (run_dir / "run-agent" / "trajectories").mkdir(parents=True, exist_ok=True)
    (run_dir / "run-eval" / "logs").mkdir(parents=True, exist_ok=True)
    (run_dir / "run-eval" / "reports").mkdir(parents=True, exist_ok=True)

    return run_dir


def run_agent_batch(config, run_dir):
    """Run mini-swe-agent batch with checkpointing."""
    agent_dir = run_dir / "run-agent"
    preds_file = agent_dir / "preds.json"
    trajectories_dir = agent_dir / "trajectories"

    if preds_file.exists() and any(trajectories_dir.iterdir()):
        print("Agent output exists, skipping")
        return agent_dir

    cmd = [
        "uv", "run", "mini-extra", "swebench",
        "--subset", config["subset"],
        "--split", config["split"],
        "--model", config["model"],
        "--config", "mini-swe-agent/src/minisweagent/config/benchmarks/swebench.yaml",
        "--workers", str(config["workers"]),
        "-o", str(trajectories_dir),
    ]

    if config.get("task_slice"):
        cmd.extend(["--slice", config["task_slice"]])

    if config.get("cost_limit") and config["cost_limit"] != "0":
        cmd.extend(["--cost-limit", str(config["cost_limit"])])

    env = os.environ.copy()
    env["MSWEA_COST_TRACKING"] = "ignore_errors"

    result = subprocess.run(cmd, env=env, cwd=PROJECT_ROOT)
    if result.returncode != 0:
        raise RuntimeError(f"Agent run failed with code {result.returncode}")

    candidate_paths = [
        trajectories_dir / "preds.json",
        agent_dir / "preds.json",
    ]
    source_preds = next((path for path in candidate_paths if path.exists()), None)
    if source_preds is None:
        source_preds = next(iter(trajectories_dir.rglob("preds.json")), None)

    if source_preds is None:
        raise FileNotFoundError(f"Could not find preds.json under {trajectories_dir}")

    if source_preds != preds_file:
        shutil.copy2(source_preds, preds_file)

    return agent_dir


def run_swebench_eval(config, preds_path, run_dir):
    """Run SWE-bench evaluation with checkpointing."""
    eval_dir = run_dir / "run-eval"
    summary_path = _find_eval_summary(eval_dir)

    if summary_path is not None:
        print("Evaluation output exists, skipping")
        return eval_dir

    run_id = run_dir.name
    cmd = [
        "python", "-m", "swebench.harness.run_evaluation",
        "--dataset_name", config["dataset_name"],
        "--predictions_path", str(preds_path.resolve()),
        "--max_workers", str(config["workers"]),
        "--run_id", run_id,
    ]

    env = os.environ.copy()
    result = subprocess.run(cmd, env=env, cwd=eval_dir)
    if result.returncode != 0:
        raise RuntimeError(f"Evaluation failed with code {result.returncode}")

    if _find_eval_summary(eval_dir) is None:
        raise FileNotFoundError(f"Could not find evaluation summary JSON in {eval_dir}")

    return eval_dir


def collect_metrics(eval_dir):
    """Parse evaluation reports and extract metrics."""
    metrics = {
        "resolved": 0,
        "completed": 0,
        "total": 0,
        "pass_rate": 0.0,
        "summary_path": None,
    }

    summary_path = _find_eval_summary(eval_dir)
    if summary_path is None:
        return metrics

    try:
        with open(summary_path) as f:
            report = json.load(f)
        if isinstance(report, dict):
            metrics["total"] = report.get("total_instances", 0)
            metrics["completed"] = report.get("completed_instances", 0)
            metrics["resolved"] = report.get("resolved_instances", 0)
            if metrics["completed"] > 0:
                metrics["pass_rate"] = metrics["resolved"] / metrics["completed"]
            metrics["summary_path"] = str(summary_path)
    except Exception as e:
        print(f"Could not parse {summary_path}: {e}")

    return metrics


def log_mlflow_run(run_id, config, metrics, artifact_uri):
    """Log run to MLflow."""
    try:
        import mlflow

        tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
        if tracking_uri:
            mlflow.set_tracking_uri(tracking_uri)

        mlflow.set_experiment("swe-bench-eval")
        with mlflow.start_run(run_name=run_id):
            mlflow.log_params({
                "split": config["split"],
                "subset": config["subset"],
                "workers": config["workers"],
                "model": config["model"],
                "task_slice": config.get("task_slice") or "",
                "cost_limit": config["cost_limit"],
                "dataset_name": config["dataset_name"],
            })
            mlflow.log_metrics({
                "resolved": metrics["resolved"],
                "completed": metrics["completed"],
                "total": metrics["total"],
                "pass_rate": metrics["pass_rate"],
            })
            mlflow.set_tag("run_id", run_id)
            artifact_path = Path(artifact_uri)
            if artifact_path.is_dir():
                mlflow.log_artifacts(str(artifact_path), artifact_path="run")
            elif artifact_path.exists():
                mlflow.log_artifact(str(artifact_path))
    except ImportError:
        print("MLflow not available")

