# Multi-stage Dockerfile for dbt
FROM python:3.11-slim as base

# Build stage
FROM base as builder

# Install system dependencies for building
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy requirements and install dbt dependencies
COPY requirements/dbt-requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/dbt-requirements.txt

# Runtime stage
FROM base as runtime

# Install runtime system dependencies
RUN apt-get update && apt-get install -y \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r dbt \
    && useradd -r -g dbt -d /home/dbt -m dbt

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Set working directory
WORKDIR /usr/app/dbt

# Copy dbt project files
COPY --chown=dbt:dbt dbt/ ./
COPY --chown=dbt:dbt docker/dbt/scripts/ /opt/dbt/scripts/

# Make scripts executable
RUN chmod +x /opt/dbt/scripts/*.sh

# Switch to dbt user
USER dbt

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD dbt --version || exit 1

# Set entrypoint
ENTRYPOINT ["/opt/dbt/scripts/entrypoint.sh"]