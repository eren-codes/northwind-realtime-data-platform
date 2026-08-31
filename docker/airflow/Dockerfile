ARG DOCKER_CLI_IMAGE=docker:29.7.2-cli
FROM ${DOCKER_CLI_IMAGE} AS docker_cli

FROM apache/airflow:3.3.1

ARG AIRFLOW_VERSION=3.3.1
ARG AIRFLOW_STANDARD_PROVIDER_VERSION=1.18.0

# Airflow 3 keeps the standard operators in a provider package.  Pin Airflow
# itself while installing the provider so pip cannot silently upgrade core.
USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "apache-airflow-providers-standard==${AIRFLOW_STANDARD_PROVIDER_VERSION}"

# The DAG orchestrates the project's existing Compose services.  Copy only the
# Docker client and its official CLI plugins; the Docker daemon remains on the
# host and is reached through /var/run/docker.sock.
USER root
COPY --from=docker_cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker_cli /usr/local/libexec/docker/cli-plugins/ \
    /usr/local/libexec/docker/cli-plugins/

RUN docker --version && docker compose version

USER airflow
