# Evaluation Pipeline Report

Airflow pipeline that runs mini-swe-agent on SWE-bench, evaluates the patches, and logs results to MLflow.

## Pipeline

DAG `dags/evaluate_agent.py` → `prepare_run` → `run_agent` → `run_eval` → `summarize_and_log` → `upload_artifacts`.
Helpers in `src/pipeline/helpers.py`. Each run writes a self-contained folder under `runs/<run-id>/`.

## Params

`split`, `subset`, `workers`, `model`, `task_slice`, `run_id`, `cost_limit`, `use_docker_operator`, `docker_image`.
`run_id` is auto-generated from timestamp + model hash when omitted.

## Run layout

```text
runs/<run-id>/
  config.json  manifest.json  metrics.json
  run-agent/   preds.json  trajectories/  logs/
  run-eval/    logs/  reports/  *.json
```

`manifest.json` points at the key files and records the execution mode, so the folder alone is enough to reconstruct a run.

## Hardening

- Checkpointing: skip agent if `preds.json` exists, skip eval if a summary json exists.
- stdout/stderr captured to per-step log files.
- Same retry/timeout defaults on all tasks: `retries=1`, `retry_delay=2m`, `execution_timeout=6h`.
- Command timeouts via `PIPELINE_AGENT_TIMEOUT_SECONDS` / `PIPELINE_EVAL_TIMEOUT_SECONDS`.

## Execution isolation

`run_agent` / `run_eval` are Python tasks that shell out. The `use_docker_operator` param picks the command path:

- `false` (default): call the project venv binaries directly (`.venv/bin/mini-extra`, `.venv/bin/python`).
- `true`: route through `scripts/mini-swe-bench-batch.sh` / `scripts/swe-bench-eval.sh` — the same entrypoints a container would call. Both paths still run inside the scheduler today; the flag does not spawn a separate container yet.

The top-level `Dockerfile` is the intended runner image, and the scheduler already mounts the Docker socket, so wrapping these two steps as real Airflow `DockerOperator` tasks (or `KubernetesPodOperator` at scale) is the remaining isolation work.

## MLflow

Logs params, metrics (`resolved`, `completed`, `total`, `pass_rate`), and full run artifacts to experiment `swe-bench-eval`.
Pinned to `2.17.0` (3.x rejects the `mlflow:5000` host header). Logging is non-fatal — the run folder stays the source of truth.
If a 3.x server touched the `mlflow-data` volume before, wipe it once (`docker volume rm mlops-assignment-e2e-ml-pipeline_mlflow-data`) so 2.17.0 can build a clean schema.

## Deployment

```bash
cp .env.example .env
docker compose build airflow-webserver airflow-scheduler airflow-init
docker compose up airflow-init
docker compose up -d
```

Airflow: `http://localhost:8080` · MLflow: `http://localhost:5000`

Required `.env` values on a Linux VM:

- `AIRFLOW_UID` = `id -u` (keeps `runs/` writable).
- `DOCKER_GID` = `stat -c '%g' /var/run/docker.sock` (eval needs Docker).
- `AIRFLOW_WEBSERVER_SECRET_KEY` = stable value (fixes 403 served-logs).

The scheduler runs `privileged` with the docker socket mounted so eval can start SWE-bench containers.
Agent/eval call `.venv/bin/*` directly (more reliable than `uv run` in-container).

## Object storage (MinIO)

`upload_artifacts` pushes the whole `runs/<run-id>/` folder to a local MinIO bucket (S3-compatible) and records the URI in `manifest.json` and as an MLflow `s3_uri` tag. MinIO runs as its own compose service — API on `:9000`, console on `http://localhost:9001` (default login `minioadmin`/`minioadmin`). The upload uses path-style addressing and auto-creates the bucket on first run.

It's optional: with `S3_BUCKET` empty the step skips cleanly, and any upload error is non-fatal since the local run folder is the source of truth. Configure via `.env`: `S3_BUCKET`, `S3_PREFIX`, `S3_ENDPOINT_URL` (`http://minio:9000`), `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, and `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD`.

## Pending

- Real Airflow `DockerOperator` tasks for `run_agent` / `run_eval` built on the top-level `Dockerfile`. The `use_docker_operator` flag is the stepping stone — it already exercises the `scripts/*.sh` entrypoints a container would run, so the switch is mostly wiring plus mounting `runs/` into the task container.

