# Evaluation Pipeline Report

This is an Airflow pipeline that runs mini-swe-agent on a slice of SWE-bench, evaluates the patches it produces, and records everything to MLflow and a durable local run folder.

## How it fits together

The DAG lives in `dags/evaluate_agent.py` and runs five steps in order: `prepare_run`, `run_agent`, `run_eval`, `summarize_and_log`, and `upload_artifacts`. Most of the real work sits in `src/pipeline/helpers.py` — the DAG file mainly wires the tasks together and passes data between them. Every run gets its own folder under `runs/<run-id>/`, and that folder is meant to be the handoff artifact: it has enough to rebuild the whole picture on its own.

## Parameters

The DAG is triggered with `split`, `subset`, `workers`, `model`, `task_slice`, `run_id`, and `cost_limit`. If you don't pass a `run_id`, one is generated from the timestamp plus a short model hash so runs never collide.

## What a run looks like on disk

```text
runs/<run-id>/
  config.json  manifest.json  metrics.json
  run-agent/   preds.json  trajectories/  logs/
  run-eval/    logs/  reports/  *.json
```

`manifest.json` is the index — it points at the important files and records how the run was executed, so the folder alone is enough to reconstruct what happened.

## Making runs durable

A few things make reruns and debugging less painful:

- The expensive steps checkpoint themselves — the agent skips if `preds.json` already exists, and eval skips if it already has a summary json.
- Each step's stdout and stderr are captured into per-step log files.
- Every task shares the same policy: one retry, a two-minute delay, and a six-hour ceiling. The underlying commands also get their own timeouts via `PIPELINE_AGENT_TIMEOUT_SECONDS` and `PIPELINE_EVAL_TIMEOUT_SECONDS`.

## How the agent and eval run

There are two execution modes, chosen at deploy time with the `EXECUTION_MODE` env var. In `local` mode (the default) `run_agent` and `run_eval` are Python tasks that call the project's venv binaries directly (`.venv/bin/mini-extra`, `.venv/bin/python`) inside the scheduler. In `docker` mode they become real Airflow `DockerOperator` tasks that run the top-level `Dockerfile` image and execute the parameterized `scripts/*.sh` inside it — genuinely isolated from the scheduler.

Because the operator launches sibling containers through the host Docker daemon, it bind-mounts `${HOST_PROJECT_DIR}/runs` into the container so outputs land in the same `runs/<run-id>/` the other tasks read, and the eval task also gets the Docker socket so the SWE-bench harness can start its per-instance test containers. To turn it on: build the runner image once (`docker compose build runner`), set `EXECUTION_MODE=docker` and `HOST_PROJECT_DIR` to the repo's absolute path, then restart. If the Docker provider or `HOST_PROJECT_DIR` isn't available it quietly falls back to local mode, so a misconfig never wedges the DAG. (`KubernetesPodOperator` would be the same shape at cluster scale.)

## MLflow

Each run logs its params, the headline metrics (`resolved`, `completed`, `total`, `pass_rate`), and the full run folder as artifacts under the `swe-bench-eval` experiment. MLflow is pinned to `2.17.0` on purpose — the 3.x server rejects the `mlflow:5000` host header from inside the compose network. Logging is also non-fatal: `metrics.json` and `manifest.json` are written before MLflow is even contacted, so a tracking hiccup just prints a warning instead of failing the run. One gotcha to know about — if a 3.x server ever wrote to the `mlflow-data` volume, wipe it once so 2.17.0 can build a clean schema.

## Object storage (MinIO)

The final step, `upload_artifacts`, pushes the whole `runs/<run-id>/` folder into a local MinIO bucket and records the resulting URI in both `manifest.json` and an MLflow `s3_uri` tag. MinIO runs as its own compose service, with the API on `:9000` and a console at `http://localhost:9001` (default login `minioadmin`/`minioadmin`). The upload uses path-style addressing and creates the bucket on first use, so a fresh MinIO just works. It's entirely optional — leave `S3_BUCKET` empty and the step skips, and any upload error is swallowed with a warning since the local folder is still the source of truth.

## Running it

```bash
cp .env.example .env
docker compose build airflow-webserver airflow-scheduler airflow-init
docker compose up airflow-init
docker compose up -d
```

Airflow ends up on `http://localhost:8080` and MLflow on `http://localhost:5000`. On a Linux VM a few `.env` values matter: set `AIRFLOW_UID` to your own `id -u` so the mounted `runs/` stays writable, set `DOCKER_GID` to the docker socket's group (`stat -c '%g' /var/run/docker.sock`) so eval can reach Docker, and give `AIRFLOW_WEBSERVER_SECRET_KEY` a stable value so task logs don't 403. The scheduler runs privileged with the socket mounted for the same reason.

