import os
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

# Execution mode is decided at DAG-parse time (Airflow can't branch topology on a runtime param).
# 'local' -> Python tasks; 'docker' -> real DockerOperator tasks on the runner image.
try:
    from airflow.providers.docker.operators.docker import DockerOperator
    from docker.types import Mount
    DOCKER_OPERATOR_AVAILABLE = True
except Exception:
    DOCKER_OPERATOR_AVAILABLE = False

EXECUTION_MODE = os.environ.get("EXECUTION_MODE", "local").strip().lower()
RUNNER_IMAGE = os.environ.get("RUNNER_IMAGE", "mlops-assignment-runner:latest")
HOST_PROJECT_DIR = os.environ.get("HOST_PROJECT_DIR", "").strip()
USE_DOCKER = EXECUTION_MODE == "docker" and DOCKER_OPERATOR_AVAILABLE and bool(HOST_PROJECT_DIR)
if EXECUTION_MODE == "docker" and not USE_DOCKER:
    print(
        "EXECUTION_MODE=docker set but DockerOperator or HOST_PROJECT_DIR is unavailable; "
        "falling back to local Python execution."
    )


def _prep_xcom(field):
    return "{{ ti.xcom_pull(task_ids='prepare_run')['" + field + "'] }}"


def _prep_cfg(field):
    return "{{ ti.xcom_pull(task_ids='prepare_run')['config']['" + field + "'] }}"


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
            "execution_mode": config.get("execution_mode"),
            "runner_image": config.get("runner_image"),
        }

        manifest_path = run_dir / "manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

        return {"run_id": run_id, "run_dir": str(run_dir), "config": config}

    @task(
        task_id="run_agent",
        retries=DEFAULT_RETRIES,
        retry_delay=DEFAULT_RETRY_DELAY,
        execution_timeout=DEFAULT_EXEC_TIMEOUT,
    )
    def run_agent_py(prepare_info):
        """Run mini-swe-agent locally (venv or scripts inside the scheduler)."""
        run_dir = Path(prepare_info["run_dir"])
        config = prepare_info["config"]
        run_agent_batch(config, run_dir)
        return {"run_dir": str(run_dir)}

    @task(
        task_id="run_eval",
        retries=DEFAULT_RETRIES,
        retry_delay=DEFAULT_RETRY_DELAY,
        execution_timeout=DEFAULT_EXEC_TIMEOUT,
    )
    def run_eval_py(prepare_info):
        """Run SWE-bench evaluation locally."""
        run_dir = Path(prepare_info["run_dir"])
        config = prepare_info["config"]
        preds_path = run_dir / "run-agent" / "preds.json"
        run_swebench_eval(config, preds_path, run_dir)
        return {"run_dir": str(run_dir)}

    @task(
        retries=DEFAULT_RETRIES,
        retry_delay=DEFAULT_RETRY_DELAY,
        execution_timeout=DEFAULT_EXEC_TIMEOUT,
    )
    def summarize_and_log(prepare_info):
        """Parse metrics and log to MLflow."""
        run_dir = Path(prepare_info["run_dir"])
        run_id = prepare_info["run_id"]
        config = prepare_info["config"]
        eval_dir = run_dir / "run-eval"

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
                "execution_mode": config.get("execution_mode"),
                "runner_image": config.get("runner_image"),
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
    def upload_artifacts(prepare_info):
        """Upload the full run folder to object storage (skips if S3 is not configured)."""
        run_dir = Path(prepare_info["run_dir"])
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

    if USE_DOCKER:
        runs_mount = Mount(source=f"{HOST_PROJECT_DIR}/runs", target="/work/runs", type="bind")
        sock_mount = Mount(source="/var/run/docker.sock", target="/var/run/docker.sock", type="bind")

        agent = DockerOperator(
            task_id="run_agent",
            image=RUNNER_IMAGE,
            auto_remove="success",
            mount_tmp_dir=False,
            mounts=[runs_mount],
            environment={
                "NEBIUS_API_KEY": os.environ.get("NEBIUS_API_KEY", ""),
                "MSWEA_COST_TRACKING": "ignore_errors",
                "SUBSET": _prep_cfg("subset"),
                "SPLIT": _prep_cfg("split"),
                "MODEL": _prep_cfg("model"),
                "WORKERS": _prep_cfg("workers"),
                "TASK_SLICE": "{{ ti.xcom_pull(task_ids='prepare_run')['config']['task_slice'] or '' }}",
                "RUN_ID": _prep_xcom("run_id"),
            },
            command=(
                "bash -lc 'set -e; "
                "bash /mlops-assignment/scripts/mini-swe-bench-batch.sh "
                "\"$SUBSET\" \"$SPLIT\" \"$MODEL\" \"$TASK_SLICE\" \"$WORKERS\" "
                "\"/work/runs/$RUN_ID/run-agent/trajectories\"; "
                "cp \"/work/runs/$RUN_ID/run-agent/trajectories/preds.json\" "
                "\"/work/runs/$RUN_ID/run-agent/preds.json\"'"
            ),
            retries=DEFAULT_RETRIES,
            retry_delay=DEFAULT_RETRY_DELAY,
            execution_timeout=DEFAULT_EXEC_TIMEOUT,
        )

        evaluate = DockerOperator(
            task_id="run_eval",
            image=RUNNER_IMAGE,
            auto_remove="success",
            mount_tmp_dir=False,
            mounts=[runs_mount, sock_mount],
            environment={
                "DATASET_NAME": _prep_cfg("dataset_name"),
                "WORKERS": _prep_cfg("workers"),
                "RUN_ID": _prep_xcom("run_id"),
            },
            command=(
                "bash -lc 'set -e; cd \"/work/runs/$RUN_ID/run-eval\"; "
                "bash /mlops-assignment/scripts/swe-bench-eval.sh "
                "\"$DATASET_NAME\" \"/work/runs/$RUN_ID/run-agent/preds.json\" "
                "\"$WORKERS\" \"$RUN_ID\"'"
            ),
            retries=DEFAULT_RETRIES,
            retry_delay=DEFAULT_RETRY_DELAY,
            execution_timeout=DEFAULT_EXEC_TIMEOUT,
        )

        prepare >> agent >> evaluate
    else:
        agent = run_agent_py(prepare)
        evaluate = run_eval_py(prepare)
        agent >> evaluate

    summary = summarize_and_log(prepare)
    evaluate >> summary
    upload = upload_artifacts(prepare)
    summary >> upload

if AIRFLOW_AVAILABLE:
    evaluate_agent_dag()

