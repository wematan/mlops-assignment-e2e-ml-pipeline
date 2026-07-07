FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
 && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

USER airflow
WORKDIR /opt/airflow/project

COPY pyproject.toml uv.lock ./
RUN uv sync --locked

# mlflow + boto3 are imported directly in the Airflow task process
RUN pip install --no-cache-dir 'mlflow==2.17.0' boto3

# DockerOperator support (provider + docker SDK client)
RUN pip install --no-cache-dir apache-airflow-providers-docker docker

COPY src ./src
COPY scripts ./scripts
COPY dags ./dags

