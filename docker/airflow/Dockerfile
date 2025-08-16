# Multi-stage Dockerfile for Apache Airflow
FROM apache/airflow:2.8.1-python3.11 as base

# Build stage - install dependencies
FROM base as builder
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements and install Python dependencies
COPY requirements/airflow-requirements.txt /tmp/
RUN pip install --user --no-cache-dir -r /tmp/airflow-requirements.txt

# Runtime stage
FROM base as runtime
USER root

# Install runtime system dependencies
RUN apt-get update && apt-get install -y \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy installed packages from builder stage
COPY --from=builder /home/airflow/.local /home/airflow/.local

# Copy DAGs, plugins, and configuration
COPY --chown=airflow:root airflow/dags/ ${AIRFLOW_HOME}/dags/
COPY --chown=airflow:root airflow/plugins/ ${AIRFLOW_HOME}/plugins/
COPY --chown=airflow:root airflow/config/ ${AIRFLOW_HOME}/config/

# Copy startup scripts
COPY --chown=airflow:root docker/airflow/scripts/ /opt/airflow/scripts/
RUN chmod +x /opt/airflow/scripts/*.sh

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Set entrypoint
ENTRYPOINT ["/opt/airflow/scripts/entrypoint.sh"]