import json
import os
import hashlib
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUNS_ROOT = PROJECT_ROOT / "runs"
VENV_BIN = PROJECT_ROOT / ".venv" / "bin"


def _venv_cmd(name: str) -> str:
    """Return path to venv binary if available, otherwise fall back to PATH lookup."""
    candidate = VENV_BIN / name
    if candidate.exists():
        return str(candidate)
    return name


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


def _candidate_agent_config_paths():
    env_override = os.environ.get("MINISWEAGENT_BENCHMARK_CONFIG")
    if env_override:
        yield Path(env_override).expanduser()

    yield PROJECT_ROOT.parent / "mini-swe-agent" / "src" / "minisweagent" / "config" / "benchmarks" / "swebench.yaml"
    yield PROJECT_ROOT / "mini-swe-agent" / "src" / "minisweagent" / "config" / "benchmarks" / "swebench.yaml"

    try:
        import minisweagent.config as minisweagent_config

        yield Path(minisweagent_config.__file__).resolve().parent / "benchmarks" / "swebench.yaml"
    except ImportError:
        pass


def resolve_agent_config_path():
    candidates = []
    for candidate in _candidate_agent_config_paths():
        resolved = candidate.resolve(strict=False)
        candidates.append(resolved)
        if resolved.exists():
            return resolved

    print(
        "Warning: Could not find swebench.yaml for mini-swe-agent. "
        f"Checked: {candidates}. Will run with mini-swe-agent defaults. "
        "Set MINISWEAGENT_BENCHMARK_CONFIG to force a specific benchmark config."
    )
    return None


def _as_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _as_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _run_command(
    cmd: list[str],
    cwd: Path,
    env: dict[str, str],
    stdout_path: Path,
    stderr_path: Path,
    timeout_seconds: int,
):
    try:
        completed = subprocess.run(
            cmd,
            env=env,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        stdout_path.write_text(exc.stdout or "", encoding="utf-8")
        stderr_path.write_text(exc.stderr or "", encoding="utf-8")
        raise RuntimeError(
            f"Command timed out after {timeout_seconds}s: {' '.join(cmd)}"
        ) from exc

    stdout_path.write_text(completed.stdout or "", encoding="utf-8")
    stderr_path.write_text(completed.stderr or "", encoding="utf-8")
    if completed.returncode != 0:
        stderr_tail = (completed.stderr or "").strip()[-2000:]
        raise RuntimeError(
            f"Command failed with code {completed.returncode}: {' '.join(cmd)}\n--- stderr ---\n{stderr_tail}"
        )


def build_run_config(params):
    """Build config dict from Airflow params."""
    agent_config_path = resolve_agent_config_path()
    config: dict[str, Any] = {
        "split": str(params.get("split", "test")),
        "subset": str(params.get("subset", "verified")),
        "workers": int(params.get("workers", 4)),
        "model": str(params.get("model", "nebius/moonshotai/Kimi-K2.6")),
        "task_slice": _normalize_optional(params.get("task_slice")),
        "cost_limit": str(params.get("cost_limit", "0")),
        "dataset_name": _dataset_name_for_subset(params.get("subset", "verified")),
        "agent_config_path": str(agent_config_path) if agent_config_path else None,
        "use_docker_operator": _as_bool(params.get("use_docker_operator", False)),
        "docker_image": str(params.get("docker_image", "mlops-assignment-runner:latest")),
        "timestamp": datetime.now().isoformat(),
    }
    return config


def generate_run_id(config: dict[str, Any]):
    """Generate unique run ID from model and timestamp."""
    model_short = config["model"].replace("/", "__")[:20]
    ts = datetime.fromisoformat(config["timestamp"]).strftime("%Y%m%d_%H%M%S")
    model_hash = hashlib.md5(model_short.encode()).hexdigest()[:8]
    return f"{ts}_{model_short}_{model_hash}"


def prepare_run_dir(run_id: str, config: dict[str, Any]):
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


def run_agent_batch(config: dict[str, Any], run_dir: Path):
    """Run mini-swe-agent batch with checkpointing."""
    agent_dir = run_dir / "run-agent"
    preds_file = agent_dir / "preds.json"
    trajectories_dir = agent_dir / "trajectories"
    logs_dir = agent_dir / "logs"
    config_path_raw = config.get("agent_config_path")
    config_path = Path(config_path_raw).expanduser() if config_path_raw else None
    logs_dir.mkdir(parents=True, exist_ok=True)

    if preds_file.exists() and any(trajectories_dir.iterdir()):
        print("Agent output exists, skipping")
        return agent_dir

    use_docker_operator = bool(config.get("use_docker_operator", False))
    if use_docker_operator:
        cmd = [
            "bash",
            str((PROJECT_ROOT / "scripts" / "mini-swe-bench-batch.sh").resolve()),
            str(config["subset"]),
            str(config["split"]),
            str(config["model"]),
            str(config.get("task_slice") or ""),
            str(config["workers"]),
            str(trajectories_dir.resolve()),
        ]
    else:
        cmd = [
            _venv_cmd("mini-extra"), "swebench",
            "--subset", config["subset"],
            "--split", config["split"],
            "--model", config["model"],
            "--workers", str(config["workers"]),
            "-o", str(trajectories_dir),
        ]

        if config_path and config_path.exists():
            cmd.extend(["--config", str(config_path)])

        if config.get("task_slice"):
            cmd.extend(["--slice", config["task_slice"]])

        if config.get("cost_limit") and config["cost_limit"] != "0":
            cmd.extend(["--cost-limit", str(config["cost_limit"])])

    env = os.environ.copy()
    env["MSWEA_COST_TRACKING"] = "ignore_errors"
    timeout_seconds = int(os.environ.get("PIPELINE_AGENT_TIMEOUT_SECONDS", "21600"))

    _run_command(
        cmd=cmd,
        env=env,
        cwd=PROJECT_ROOT,
        stdout_path=logs_dir / "agent.stdout.log",
        stderr_path=logs_dir / "agent.stderr.log",
        timeout_seconds=timeout_seconds,
    )

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


def run_swebench_eval(config: dict[str, Any], preds_path: Path, run_dir: Path):
    """Run SWE-bench evaluation with checkpointing."""
    eval_dir = run_dir / "run-eval"
    summary_path = _find_eval_summary(eval_dir)

    if summary_path is not None:
        print("Evaluation output exists, skipping")
        return eval_dir

    run_id = run_dir.name
    dataset_name = str(config["dataset_name"])
    max_workers = str(config["workers"])
    logs_dir = eval_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    use_docker_operator = bool(config.get("use_docker_operator", False))
    if use_docker_operator:
        cmd = [
            "bash",
            str((PROJECT_ROOT / "scripts" / "swe-bench-eval.sh").resolve()),
            dataset_name,
            str(preds_path.resolve()),
            max_workers,
            run_id,
        ]
    else:
        cmd = [
            _venv_cmd("python"), "-m", "swebench.harness.run_evaluation",
            "--dataset_name", dataset_name,
            "--predictions_path", str(preds_path.resolve()),
            "--max_workers", max_workers,
            "--run_id", run_id,
        ]

    env = os.environ.copy()
    timeout_seconds = int(os.environ.get("PIPELINE_EVAL_TIMEOUT_SECONDS", "14400"))
    _run_command(
        cmd=cmd,
        env=env,
        cwd=eval_dir,
        stdout_path=logs_dir / "eval.stdout.log",
        stderr_path=logs_dir / "eval.stderr.log",
        timeout_seconds=timeout_seconds,
    )

    if _find_eval_summary(eval_dir) is None:
        raise FileNotFoundError(f"Could not find evaluation summary JSON in {eval_dir}")

    return eval_dir


def collect_metrics(eval_dir: Path):
    """Parse evaluation reports and extract metrics."""
    metrics: dict[str, Any] = {
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
            metrics["total"] = _as_int(report.get("total_instances", 0))
            metrics["completed"] = _as_int(report.get("completed_instances", 0))
            metrics["resolved"] = _as_int(report.get("resolved_instances", 0))
            if metrics["completed"] > 0:
                metrics["pass_rate"] = metrics["resolved"] / metrics["completed"]
            metrics["summary_path"] = str(summary_path)
    except Exception as e:
        print(f"Could not parse {summary_path}: {e}")

    return metrics


def log_mlflow_run(run_id: str, config: dict[str, Any], metrics: dict[str, Any], artifact_uri: str):
    """Log run to MLflow."""
    def _log_with_mlflow_module(mlflow_module):
        tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
        if tracking_uri:
            mlflow_module.set_tracking_uri(tracking_uri)

        mlflow_module.set_experiment("swe-bench-eval")
        with mlflow_module.start_run(run_name=run_id):
            mlflow_module.log_params(
                {
                    "split": config["split"],
                    "subset": config["subset"],
                    "workers": config["workers"],
                    "model": config["model"],
                    "task_slice": config.get("task_slice") or "",
                    "cost_limit": config["cost_limit"],
                    "dataset_name": config["dataset_name"],
                    "use_docker_operator": config.get("use_docker_operator", False),
                    "docker_image": config.get("docker_image", ""),
                }
            )
            mlflow_module.log_metrics(
                {
                    "resolved": metrics["resolved"],
                    "completed": metrics["completed"],
                    "total": metrics["total"],
                    "pass_rate": metrics["pass_rate"],
                }
            )
            mlflow_module.set_tag("run_id", run_id)
            mlflow_module.set_tag("artifact_scope", "full_run_dir")
            artifact_path = Path(artifact_uri)
            if artifact_path.is_dir():
                mlflow_module.log_artifacts(str(artifact_path), artifact_path="run")
            elif artifact_path.exists():
                mlflow_module.log_artifact(str(artifact_path))

    try:
        import mlflow
    except ImportError:
        print("MLflow not available in Airflow environment; skipping MLflow logging")
        return

    try:
        _log_with_mlflow_module(mlflow)
    except Exception as exc:
        print(f"MLflow logging failed (run artifacts are still in {artifact_uri}): {exc}")

