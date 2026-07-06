# Evaluation Pipeline Report

This is the current state of the assignment implementation after finishing the first working version and adding production-style hardening.

## What is working now

The main DAG is `dags/evaluate_agent.py` with four tasks:

1. `prepare_run`
2. `run_agent`
3. `run_eval`
4. `summarize_and_log`

Helper logic lives in `src/pipeline/helpers.py`.

The pipeline creates a run folder under `runs/<run-id>/`, runs mini-swe-agent, runs SWE-bench evaluation, writes metrics/manifest, and logs the run to MLflow.

## Parameters and run identity

The DAG accepts:

- `split`
- `subset`
- `workers`
- `model`
- `task_slice`
- `run_id`
- `cost_limit`
- `use_docker_operator`
- `docker_image`

If `run_id` is not provided, it is generated from timestamp + model hash.

## Run artifacts and reproducibility

Each run is structured like this:

```text
runs/<run-id>/
  config.json
  manifest.json
  metrics.json
  run-agent/
    preds.json
    trajectories/
    logs/
      agent.stdout.log
      agent.stderr.log
  run-eval/
    logs/
      eval.stdout.log
      eval.stderr.log
    reports/
    *.json
```

`manifest.json` records the important paths and execution mode (`use_docker_operator`, `docker_image`) so a teammate can reconstruct what happened from one folder.

## Hardening done in this phase

- Added checkpointing for expensive steps:
  - skip agent run when `run-agent/preds.json` and trajectories already exist
  - skip evaluation when a summary json already exists in `run-eval/`
- Added command-level stdout/stderr capture into run-local log files.
- Added task retries/timeouts with the same defaults across all DAG tasks:
  - `retries=1`
  - `retry_delay=2 minutes`
  - `execution_timeout=6 hours`
- Added helper-level command timeouts through env vars:
  - `PIPELINE_AGENT_TIMEOUT_SECONDS` (default `21600`)
  - `PIPELINE_EVAL_TIMEOUT_SECONDS` (default `14400`)

## MLflow tracking

The DAG logs each run to MLflow experiment `swe-bench-eval` with:

- parameters (including execution-mode flags)
- metrics (`resolved`, `completed`, `total`, `pass_rate`)
- `run_id` tag
- full run artifacts via `mlflow.log_artifacts(runs/<run-id>/...)`

In short: MLflow is used for comparison, and `runs/<run-id>/` is still the source-of-truth handoff folder.

MLflow is pinned to `2.17.0` (both server and client). Newer 3.x server builds reject the
`Host: mlflow:5000` header from inside the compose network as a DNS-rebinding attempt, which
breaks logging. The pin avoids that. MLflow logging is also non-fatal now: `metrics.json` and
`manifest.json` are written before the MLflow call, so a tracking hiccup only prints a warning
instead of failing the run.

> Note: if a 3.x MLflow server ran against the `mlflow-data` volume first, its SQLite DB carries
> an alembic revision 2.17.0 cannot resolve (`Can't locate revision identified by ...`). Wipe the
> volume once so the pinned server can create a clean schema:
>
> ```bash
> docker compose stop mlflow
> docker compose rm -f mlflow
> docker volume rm mlops-assignment-e2e-ml-pipeline_mlflow-data
> docker compose up -d mlflow
> ```

## Deployment setup used

Production-style local setup is via:

- `docker-compose.yaml` (Airflow + Postgres + MLflow)
- `docker/airflow/Dockerfile` (Airflow image with `uv`, project deps, and Docker provider)
- `.env.example` (required env variables)

Bring it up with:

```bash
cp .env.example .env
docker compose build airflow-webserver airflow-scheduler airflow-init
docker compose up airflow-init
docker compose up -d
```

UI endpoints:

- Airflow: `http://localhost:8080`
- MLflow: `http://localhost:5000`

## Environment setup that actually matters

A few things need to be right in `.env` before the compose stack behaves on a Linux VM:

- `AIRFLOW_UID` should be your host user id (`id -u`). Running the containers as your own user
  keeps the bind-mounted `runs/` folder writable and avoids permission clashes.
- `DOCKER_GID` should match the docker socket group (`stat -c '%g' /var/run/docker.sock`). The
  `run_eval` step shells out to the SWE-bench harness, which talks to the Docker daemon to spin up
  one container per test instance.
- `AIRFLOW_WEBSERVER_SECRET_KEY` must be set to a stable value. All Airflow services share it, which
  fixes the "Could not read served logs / 403" issue when viewing task logs.

Two more compose details worth calling out:

- The scheduler runs with `privileged: true` and mounts `/var/run/docker.sock`. That is what lets
  the evaluation step reach Docker for the SWE-bench containers.
- The agent and eval steps call the project venv binaries directly (`.venv/bin/mini-extra`,
  `.venv/bin/python`) instead of `uv run`, which was unreliable inside the Airflow container.

## What is intentionally pending


- Full Airflow `DockerOperator` task wiring is still pending.
  - Right now, `use_docker_operator` is implemented as a feature flag in config/manifest/execution path, with subprocess fallback still in place.
- S3/object storage upload step is intentionally deferred for now.

This keeps the current version stable and reproducible while leaving a clean next step for full production isolation and remote artifact durability.

