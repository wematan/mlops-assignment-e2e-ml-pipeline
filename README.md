# Home assignment: Evaluation pipeline for coding-agent experiments

**What**: Home assignment.

**Where**: Nebius Academy course [AI Performance Engineering](https://academy.nebius.com/ai-engineering-il), MLOps module, lecture #6, "End-to-end ML pipeline".

**Author**: Simon Karasik.

**Learning objective**: Get hands-on experience turning an ad-hoc coding-agent evaluation script into an automated, observable, versioned, and durable Airflow pipeline with a structured data footprint: datasets, artifacts, metadata, metrics, logs, and trajectories.

**Non-goals**: Deep dive into Airflow internals, SWE-bench internals, LLM fine-tuning theory, or sandbox infrastructure. The point is to connect the pieces into a usable evaluation system. Fine-tuning is left as an optional extension.

**Inspired by**: https://github.com/GlebBerjoskin/mlops-assignment

---

## Legend

Imagine you are an MLOps engineer on a team that builds better coding agents. Think Claude Code, Codex, Cursor, OpenCode, mini-swe-agent, and similar systems.

Agent quality depends on two broad things:

1. **Harness**: the agent loop, prompts, tools, skills, retries, subagents, context management, and execution environment.
2. **Model**: the LLM that powers the harness, including decoding parameters and fine-tuned variants.

Your researchers want to experiment with both. Typical research loops look like this:

1. tweak a prompt or harness setting -> run the agent -> evaluate generated patches
2. fine-tune a model -> deploy it -> run the agent -> evaluate generated patches

Quality is measured on [SWE-bench](https://www.swebench.com/)-like tasks: the agent receives a real GitHub issue inside an isolated environment, tries to solve it, produces a patch, and the patch is judged by real unit tests.

Right now the researchers have several scripts on one VM. Someone SSHes in, runs them by hand, waits, copies logs, and pastes numbers into a doc. One experiment at a time. No queue. No durable run history. No reliable way to answer "which config produced this result?" or "why did this run fail?"

So, the team needs your help to turn these ad-hoc scripts into reliable, multi-user pipelines.

**Assignment scope**. Productionize the first loop: evaluate the agent reliably. Researchers should be able to submit a batch of evaluation experiments, run them on a remote VM, inspect MLflow results and mini-swe-agent trajectories, and rerun the same config later.

**For avid learners**. Design & productionize train-model & evaluate-agent part.
---

## Why This Matters

By the end of the assignment you should be able to:

- Model an ML experiment as a pipeline with explicit inputs, outputs, retries, and dependencies.
- Use Airflow for orchestration instead of manual shell ordering.
- Track experiment configs, datasets, model IDs, metrics, artifacts, and logs in MLflow.
- Run coding-agent evaluations in user-provided Docker images and collect reproducible outputs.
- Keep Airflow code on a remote VM updated automatically from Git or S3.
- Deploy and use the mini-swe-agent trajectory viewer to inspect what happened inside an agent run.
- Compare multiple experiments without losing track of which code, prompt, dataset, and model produced each result.

If done carefully, this assignment teaches the practical MLOps discipline that research code usually lacks: durability, repeatability, provenance, and operational visibility.

---

## Target System

The mandatory assignment has one Airflow pipeline and two supporting services:

1. `evaluate-agent`: run `mini-swe-agent` on a configurable SWE-bench-like task set and save enough metrics and artifacts to reproduce and diagnose the run.
2. **MLflow**: track experiment configs, metrics, artifacts, and run metadata.
3. **Trajectory viewer**: serve mini-swe-agent trajectories so failures can be inspected visually.

There is also an optional extension:

- `train-model` and `train-model-and-evaluate-agent`: fine-tune an LLM on agent trajectories, then evaluate the resulting model with the same `evaluate-agent` pipeline.

The expected stack:

- **Airflow** as the pipeline engine.
- **MLflow** as the experiment tracker.
- **mini-swe-agent** as the research-friendly coding agent.
- **SWE-bench** or a supplied SWE-bench-like subset as the evaluation harness.
- **User-provided Docker images** as the execution environment for pipeline actions.
- **Managed LLM inference** via API, for example Nebius Token Factory Inference, AWS Bedrock, Together AI, or another OpenAI-compatible endpoint.
- Optional: **managed code sandboxes** via API, for example Nebius Token Factory Sandboxes, Daytona, Modal, or E2B.
- Optional: **managed LLM fine-tuning** via API, for example Nebius Token Factory Fine-Tuning, AWS Bedrock, Together AI, or another equivalent service.

Managed services are intentional. In real production you might self-host some pieces, for example vLLM on Kubernetes, but this assignment is about pipeline design and experiment discipline, not cluster operations.

---

## Prerequisites

- A CPU VM or workstation where you can run Airflow, MLflow, Docker, and Python jobs.
- Docker and Docker Compose.
- Python 3.11+ and `uv`.
- Access credentials for:
  - an OpenAI-compatible inference endpoint
- Optional, for extensions:
  - a fine-tuning endpoint
  - a sandbox provider
- Enough API quota to run the required experiments.

You do not need a GPU VM for the orchestration parts. The expensive work should happen behind managed APIs.

---

## Phase 0: Setup

You will work on a VM. Airflow and MLflow can run locally on that VM, and their UIs can be reached from your laptop by forwarding ports.

You usually need these ports:

- **8080**: Airflow UI
- **5000**: MLflow UI
- **8001**: mini-swe-agent trajectory viewer, or whichever port your deployment uses
- **9001**: optional object-store UI, if your scaffold uses MinIO

**VSCode or Cursor.** Use Remote-SSH, connect to the VM, and forward the ports from the editor's Ports panel.

**Plain SSH fallback.**

```bash
ssh -L 8080:localhost:8080 \
    -L 5000:localhost:5000 \
    -L 8001:localhost:8001 \
    -L 9001:localhost:9001 \
    <user>@<vm-host>
```

Once connected, set up the starter repo:

```bash
git clone <repo-url>
cd <repo-folder>
uv sync
cp .env.example .env
docker compose up -d
```

Put service credentials and endpoint URLs in `.env`. Do not commit secrets.

Airflow must run on the remote VM. Pipeline code should update automatically from Git or S3; manually SSHing into the VM and editing DAG files is not an acceptable operating model. The exact mechanism is up to you: `git-sync`, a scheduled pull, object-store sync, or another documented approach.

### What you should have in the end

- Airflow reachable at `http://localhost:8080`
- MLflow reachable at `http://localhost:5000`
- mini-swe-agent trajectory viewer reachable from your laptop browser
- `.env` configured with inference and tracking settings
- Airflow DAG code updated automatically from Git or S3
- A successful Airflow test run of a trivial DAG

---

## Starter Scaffold

The starter repo gives you just enough to begin, not a finished solution:

- Minimal VM or Docker Compose setup for Airflow and MLflow.
- A minimal `evaluate-agent` DAG that can run one hard-coded evaluation locally.
- A minimal trajectory viewer deployment hook or placeholder.
- Helper code for reading configs, calling provider APIs, and writing artifacts.

The scaffold is intentionally incomplete. Your task is to make it configurable, reliable, observable, and useful for the milestones below.

---

## Phase 1: `evaluate-agent`

Start with evaluation. This phase should be useful on its own: if you stop here, you have still built a reproducible experiment loop for comparing coding-agent harness changes.

The evaluation pipeline runs a coding agent on a configurable batch of SWE-bench-like tasks, judges the produced patches, and records enough evidence to compare runs later.

### Define the experiment contract

Before wiring the DAG, define how a researcher asks for an evaluation run.

The config should cover the knobs needed for harness experiments: task subset, prompt version, model ID, decoding settings, execution image, timeouts, and retries. It should also make the run traceable back to code, config, data, prompt, and model.

Decide where evaluation artifacts live. The exact structure is up to you, but a future teammate should be able to find the config, per-task evidence, aggregate metrics, and any external artifact references.

Example:

```text
artifacts/
  evaluate-agent/<run_id>/
    config.yaml
    task_outputs/
    metrics.json
```

### Build the DAG

The DAG should load the config, materialize the task set, run `mini-swe-agent`, judge the results with SWE-bench, and aggregate metrics into MLflow.

Use Airflow structure to expose the shape of the work. Do not hide the whole run inside one giant Python function.

Pipeline actions must execute inside user-provided Docker images. The image should be part of the run config or otherwise clearly controlled by the user. A researcher should be able to change the agent/evaluation environment by changing the image reference rather than rebuilding the Airflow VM.

Persist enough per-task information to debug, rerun, and judge the task later. At minimum, you should be able to recover what was attempted, what patch or answer was produced, whether it failed, and where the relevant evidence is.

Choose metrics that make runs comparable and diagnosable. They should cover quality, failures, runtime, and resource usage or cost where available. Examples include resolved rate, timeout rate, p95 runtime, token usage, and execution-environment failure rate.

### Run evaluation experiments

Run a small but meaningful experiment matrix with a fixed evaluation task set:

1. **Baseline**: one prompt, one model, one decoding config.
2. **Prompt versions**: three harness prompt variants. For example: baseline, concise, and repair-heavy.
3. **Temperature**: at least three values. For example: `0.0`, `0.2`, and `0.7`.

Keep unrelated variables fixed inside each comparison. The point is not only to get a best score; the point is to make a comparison you can defend.

### Read the outcomes

Use MLflow and your saved artifacts to answer:

- Which prompt performed best on the chosen task set?
- Did temperature change quality, failure rate, runtime, or cost?
- Which failures are agent failures, execution-environment failures, or evaluation failures?
- Can someone rerun the exact same experiment from the committed config?

Open the trajectory viewer and inspect at least a few successful and failed runs. The viewer should make the agent's decisions, tool calls, and failure modes easier to understand than raw logs alone.

Add the evaluation section to `REPORT.md`. Include the run table, a short interpretation, and one or two concrete examples of failures you inspected through the trajectory viewer or artifacts.

### What you should have in the end

- `evaluate-agent` DAG with configurable task subset, prompt, model, and decoding parameters
- Evaluation configs committed under `configs/experiments/evaluate-agent/`
- Durable per-task evidence and aggregate metrics
- MLflow runs comparing baseline, prompt versions, and temperatures
- `results/evaluate_agent_baseline.json`
- `results/evaluate_agent_experiments.json`
- Airflow screenshot showing mapped evaluation tasks (`screenshots/airflow_evaluate_agent.png`)
- MLflow screenshot showing evaluation runs side by side (`screenshots/mlflow_evaluate_agent.png`)
- Trajectory viewer screenshot showing an inspected mini-swe-agent run (`screenshots/trajectory_viewer.png`)
- A `REPORT.md` section explaining the evaluation setup and outcomes

This is a complete first milestone. A submission that only completes this phase should still demonstrate reproducible pipeline design, experiment tracking, failure diagnosis, and honest comparison.

---

## Phase 2: Reliability And Operations

This assignment is not only about green runs. It is about building pipelines someone else can trust.

### What to do

1. Add retries only where retrying is safe.
2. Add timeouts to Docker-executed actions, provider calls, and evaluation steps.
3. Make failed tasks diagnosable from Airflow logs and saved artifacts.
4. Make repeated runs reproducible from a committed config.
5. Ensure partial failures do not erase successful per-task artifacts.
6. Add a short "how to rerun" section to `REPORT.md`.

You should be able to answer:

- Which tasks failed?
- Why did they fail?
- Which model and prompt were used?
- Which dataset rows were used?
- Where is the evidence needed to debug the run?
- Can I rerun this exact config tomorrow?

### What you should have in the end

- Sensible Airflow retries and timeouts
- Per-task failure records
- Notes on code sync, Docker image selection, retries, and reruns
- Clear runbook notes in `REPORT.md`

---

## Phase 3: Report

Write `REPORT.md`. Keep it concise: 2-3 pages is enough.

It should include:

1. Architecture overview: Airflow, MLflow, trajectory viewer, Docker execution images, inference endpoint, artifact storage.
2. Pipeline contract: key config fields and artifact layout.
3. Evaluation results: baseline, prompt experiments, temperature experiments.
4. Reliability notes: VM deployment, automatic code updates from Git/S3, retries, timeouts, and failure handling.
5. Cost and runtime summary.
6. What you would improve with more time. Be specific.

Do not hide bad results. A weak prompt experiment or failed task batch is still useful if you can explain what happened and show reliable measurement.

---

## Optional Extensions

If you finish the mandatory scope and want to go further, extend the same experiment platform to model training.

### `train-model`

Build a DAG that fine-tunes an LLM on a configurable subset of agent trajectories, such as `SWE-rebench-openhands-trajectories`. It should prepare data, launch the provider fine-tuning job, track the provider job and resulting model ID, and log training metadata to MLflow.

### `train-model-and-evaluate-agent`

Compose training with the mandatory `evaluate-agent` pipeline. The fine-tuned model ID should flow into evaluation without manual copy-paste, and MLflow should make the relationship between training and evaluation runs visible.

Suggested experiment: train on 10 / 100 / 1000 trajectories, keep the evaluation set fixed, and compare against the mandatory evaluation baseline. The important question is whether fine-tuning improves quality enough to justify the extra system complexity.

---

## Final Deliverables

By the end of the mandatory assignment, your repo should contain:

| File or directory | What it is |
|---|---|
| `REPORT.md` | Final writeup |
| `infra/` or `docker-compose.yml` | Remote VM deployment for Airflow, MLflow, trajectory viewer, and code sync |
| `dags/evaluate_agent.py` | Configurable evaluation DAG |
| `src/` or `pipeline/` | Shared implementation code, operators, provider clients, config schemas |
| `configs/` | Reproducible configs for all evaluation runs, including execution image references |
| `.env.example` | Non-secret environment template for required services |
| `results/evaluate_agent_baseline.json` | Baseline evaluation result |
| `results/evaluate_agent_experiments.json` | Prompt and temperature experiment summary |
| `artifacts/` or external artifact URI references | Durable run evidence: task outputs, logs, trajectory references, or manifests |
| `screenshots/airflow_evaluate_agent.png` | Evaluation DAG run |
| `screenshots/mlflow_evaluate_agent.png` | MLflow comparison view for evaluation runs |
| `screenshots/mlflow_run_artifacts.png` | MLflow run with metrics, params, and artifacts |
| `screenshots/trajectory_viewer.png` | Trajectory viewer with an inspected mini-swe-agent run |

If artifacts are too large to commit, commit small indexes or manifests that point to their storage location.

Optional extension deliverables may include `dags/train_model.py`, `dags/train_model_and_evaluate_agent.py`, `results/fine_tuning_experiments.json`, and `screenshots/mlflow_fine_tuning.png`.

---

## Grading

We care more about engineering judgment and traceability than about one lucky metric. A weak result with excellent provenance and analysis is better than a pasted number nobody can reproduce.

| Area | Weight | What a strong submission shows |
|---|---:|---|
| **Remote Airflow deployment** | 15% | Airflow runs on a VM, DAG code updates automatically from Git/S3, and the setup can be reproduced without manual file edits on the VM. |
| **Docker execution model** | 15% | Pipeline actions run in user-provided Docker images, with image references controlled by config or another clear user-facing mechanism. |
| **Evaluation pipeline** | 25% | Configurable `evaluate-agent`, durable task-level evidence, SWE-bench-compatible judging, meaningful aggregate metrics, and no hidden giant script. |
| **MLflow tracking** | 15% | Runs log configs, metrics, artifacts or artifact references, code/data/model metadata, and comparison views for the experiments. |
| **Trajectory inspection** | 10% | mini-swe-agent trajectory viewer is deployed and used to inspect successful and failed runs. |
| **Experiment rigor** | 10% | Baseline, prompt, and temperature experiments are comparable, reproducible, and interpreted honestly. |
| **Report and runbook** | 10% | `REPORT.md` is concise, includes rerun instructions, explains failures, and states what would be improved next. |

---

## Practical Advice

Start small. First make one evaluation task run end to end and log one metric. Then run it inside the configured Docker image. Then fan out to a task batch. Then compare prompts and temperatures.

The most common failure mode in this assignment is trying to build the full platform in one pass. A boring pipeline that records every input and output is better than a clever one that only works once.
