# Phase 1

I started with the easy-mode version first: a plain Airflow DAG that wires the existing agent and eval commands together, writes everything into a single run folder, and leaves enough breadcrumbs to rerun or inspect the job later.

## What is in place

- `dags/evaluate_agent.py`
  - `prepare_run`
  - `run_agent`
  - `run_eval`
  - `summarize_and_log`
- `src/pipeline/helpers.py` for the small amount of plumbing around paths, checkpointing, metrics parsing, and MLflow logging.

## Params

The DAG accepts:

- `split`
- `subset`
- `workers`
- `model`
- `task_slice`
- `run_id`
- `cost_limit`

If `run_id` is not passed, it is generated automatically from timestamp + model.

## Run layout

Each run goes under `runs/<run-id>/` and currently looks like this:

```text
runs/<run-id>/
  config.json
  manifest.json
  run-agent/
    preds.json
    trajectories/
  run-eval/
    logs/
    reports/
    *.json
  metrics.json
```

The idea is that the folder itself is the handoff artifact.

## Checkpointing

This first pass already checkpoints the expensive steps:

- agent step skips if `run-agent/preds.json` is already there
- eval step skips if the eval summary json already exists in `run-eval/`

That should make retries less painful when only the last step fails.

## Local validation I ran

I added a tiny `test_helpers.py` runner and used it to verify:

- config building
- run id generation
- run directory creation
- sample metrics parsing from the provided `sample/` data

Run it with:

```bash
python test_helpers.py
```

## Notes

- MLflow logging is wired in, but whether it actually records to a UI depends on having an `MLFLOW_TRACKING_URI` or a local MLflow server available in the environment.
- I have not done the DockerOperator / docker-compose part yet. That is next once this version feels stable enough.

