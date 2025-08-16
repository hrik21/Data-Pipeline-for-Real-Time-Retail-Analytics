# Multi-stage Dockerfile for Python services
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

# Copy requirements and install Python dependencies
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Runtime stage
FROM base as runtime

# Install runtime system dependencies
RUN apt-get update && apt-get install -y \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r pipeline \
    && useradd -r -g pipeline -d /home/pipeline -m pipeline

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Set working directory
WORKDIR /usr/app

# Copy application code
COPY --chown=pipeline:pipeline src/ ./src/
COPY --chown=pipeline:pipeline config/ ./config/
COPY --chown=pipeline:pipeline docker/python/scripts/ /opt/pipeline/scripts/

# Make scripts executable
RUN chmod +x /opt/pipeline/scripts/*.sh

# Switch to pipeline user
USER pipeline

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import src; print('OK')" || exit 1

# Set entrypoint
ENTRYPOINT ["/opt/pipeline/scripts/entrypoint.sh"]