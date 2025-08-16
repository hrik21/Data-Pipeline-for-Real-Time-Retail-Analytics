#!/bin/bash
set -e

# Set default service if none provided
SERVICE_TYPE=${SERVICE_TYPE:-api}

# Wait for dependencies
echo "Checking dependencies..."
python -c "
import sys
sys.path.append('/usr/app')
try:
    from src.config.settings import get_settings
    settings = get_settings()
    print('Configuration loaded successfully')
except Exception as e:
    print(f'Configuration error: {e}')
    sys.exit(1)
"

# Start the appropriate service
case "${SERVICE_TYPE}" in
    api)
        echo "Starting API service..."
        exec python -m src.api.main
        ;;
    ingestion)
        echo "Starting ingestion service..."
        exec python -m src.ingestion.main
        ;;
    processor)
        echo "Starting data processor service..."
        exec python -m src.processing.main
        ;;
    monitor)
        echo "Starting monitoring service..."
        exec python -m src.monitoring.main
        ;;
    worker)
        echo "Starting background worker..."
        exec python -m src.worker.main
        ;;
    *)
        echo "Unknown SERVICE_TYPE: ${SERVICE_TYPE}"
        echo "Available types: api, ingestion, processor, monitor, worker"
        exit 1
        ;;
esac