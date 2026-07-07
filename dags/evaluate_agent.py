import sys
import json
from datetime import datetime
from datetime import timedelta
from pathlib import Path

try:
    from airflow.decorators import dag, task
    from airflow.operators.python import get_current_context
    AIRFLOW_AVAILABLE = True
except ImportError:  # local lint/test fallback
    AIRFLOW_AVAILABLE = False

    def dag(*_args, **_kwargs):
        def decorator(func):
            return func

        return decorator

    def task(func=None, **_kwargs):
        if func is None:
            def decorator(inner):
                return inner

            return decorator
        return func

    def get_current_context():
        return {"params": {}, "dag_run": None}

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RETRIES = 1
DEFAULT_RETRY_DELAY = timedelta(minutes=2)
DEFAULT_EXEC_TIMEOUT = timedelta(hours=6)

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.pipeline.helpers import (
    build_run_config,
    collect_metrics,
    generate_run_id,
    log_mlflow_run,
    prepare_run_dir,
    run_agent_batch,
    run_swebench_eval,
    s3_destination_uri,
    upload_run_to_s3,
)


@dag(
    dag_id="evaluate_agent",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "split": "test",
        "subset": "verified",
        "workers": 4,
        "model": "nebius/moonshotai/Kimi-K2.6",
        "task_slice": None,
        "run_id": None,
        "cost_limit": "0",
        "use_docker_operator": False,
        "docker_image": "mlops-assignment-runner:latest",
    },
)
def evaluate_agent_dag():
    @task(
        retries=DEFAULT_RETRIES,
        retry_delay=DEFAULT_RETRY_DELAY,
        execution_timeout=DEFAULT_EXEC_TIMEOUT,
    )
    def prepare_run():
        """Prepare run directory and config."""
        context = get_current_context()
        dag_run = context.get("dag_run")
        params = dict(context.get("params", {}))
        if dag_run and dag_run.conf:
            params.update(dag_run.conf)

        config = build_run_config(params)
        run_id = params.get("run_id") or generate_run_id(config)
        run_dir = prepare_run_dir(run_id, config)

        manifest = {
            "run_id": run_id,
            "created_at": datetime.now().isoformat(),
            "config": str(run_dir / "config.json"),
            "run_agent": str(run_dir / "run-agent"),
            "predictions": str(run_dir / "run-agent" / "preds.json"),
            "run_eval": str(run_dir / "run-eval"),
            "metrics": str(run_dir / "metrics.json"),
            "mlflow_experiment": "swe-bench-eval",
            "agent_config_path": config.get("agent_config_path"),
            "use_docker_operator": config.get("use_docker_operator", False),
            "docker_image": config.get("docker_image"),
        }

        manifest_path = run_dir / "manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

        return {"run_id": run_id, "run_dir": str(run_dir), "config": config}

    @task(
        retries=DEFAULT_RETRIES,
        retry_delay=DEFAULT_RETRY_DELAY,
        execution_timeout=DEFAULT_EXEC_TIMEOUT,
    )
    def run_agent(prepare_info):
        """Run mini-swe-agent."""
        run_dir = Path(prepare_info["run_dir"])
        config = prepare_info["config"]

        agent_dir = run_agent_batch(config, run_dir)

        return {
            "run_dir": str(run_dir),
            "agent_dir": str(agent_dir),
            "preds_path": str(agent_dir / "preds.json"),
        }

    @task(
        retries=DEFAULT_RETRIES,
        retry_delay=DEFAULT_RETRY_DELAY,
        execution_timeout=DEFAULT_EXEC_TIMEOUT,
    )
    def run_eval(agent_info, prepare_info):
        """Run SWE-bench evaluation."""
        run_dir = Path(agent_info["run_dir"])
        config = prepare_info["config"]
        preds_path = Path(agent_info["preds_path"])

        eval_dir = run_swebench_eval(config, preds_path, run_dir)

        return {
            "run_dir": str(run_dir),
            "eval_dir": str(eval_dir),
        }

    @task(
        retries=DEFAULT_RETRIES,
        retry_delay=DEFAULT_RETRY_DELAY,
        execution_timeout=DEFAULT_EXEC_TIMEOUT,
    )
    def summarize_and_log(eval_info, prepare_info):
        """Parse metrics and log to MLflow."""
        run_dir = Path(eval_info["run_dir"])
        eval_dir = Path(eval_info["eval_dir"])
        run_id = prepare_info["run_id"]
        config = prepare_info["config"]

        metrics = collect_metrics(eval_dir)

        metrics_path = run_dir / "metrics.json"
        with open(metrics_path, "w") as f:
            json.dump(metrics, f, indent=2)

        s3_uri = s3_destination_uri(run_id)

        manifest_path = run_dir / "manifest.json"
        if manifest_path.exists():
            with open(manifest_path) as f:
                manifest = json.load(f)
        else:
            manifest = {"run_id": run_id}

        manifest.update(
            {
                "predictions": str(run_dir / "run-agent" / "preds.json"),
                "agent_config_path": config.get("agent_config_path"),
                "eval_summary": metrics.get("summary_path"),
                "metrics": str(metrics_path),
                "use_docker_operator": config.get("use_docker_operator", False),
                "docker_image": config.get("docker_image"),
                "s3_uri": s3_uri,
                "finished_at": datetime.now().isoformat(),
            }
        )
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

        log_mlflow_run(run_id, config, metrics, str(run_dir), s3_uri=s3_uri)

        return {
            "run_id": run_id,
            "run_dir": str(run_dir),
            "metrics": metrics,
        }

    @task(
        retries=DEFAULT_RETRIES,
        retry_delay=DEFAULT_RETRY_DELAY,
        execution_timeout=DEFAULT_EXEC_TIMEOUT,
    )
    def upload_artifacts(summary_info, prepare_info):
        """Upload the full run folder to object storage (skips if S3 is not configured)."""
        run_dir = Path(summary_info["run_dir"])
        run_id = prepare_info["run_id"]

        result = upload_run_to_s3(run_dir, run_id)

        manifest_path = run_dir / "manifest.json"
        if manifest_path.exists():
            with open(manifest_path) as f:
                manifest = json.load(f)
        else:
            manifest = {"run_id": run_id}

        manifest["s3_uri"] = result.get("s3_uri")
        manifest["s3_uploaded"] = result.get("uploaded", False)
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

        return {
            "run_id": run_id,
            "run_dir": str(run_dir),
            "s3_uri": result.get("s3_uri"),
            "uploaded": result.get("uploaded", False),
        }

    prepare = prepare_run()
    agent = run_agent(prepare)
    evaluate = run_eval(agent, prepare)
    summary = summarize_and_log(evaluate, prepare)
    upload_artifacts(summary, prepare)

if AIRFLOW_AVAILABLE:
    evaluate_agent_dag()

