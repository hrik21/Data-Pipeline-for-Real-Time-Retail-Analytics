#!/bin/bash
# Generic health check script for all services

set -e

SERVICE_TYPE=${1:-"generic"}
HEALTH_CHECK_URL=${2:-"http://localhost:8080/health"}

case "${SERVICE_TYPE}" in
    airflow)
        # Check if Airflow webserver is responding
        curl -f "${HEALTH_CHECK_URL}" || exit 1
        ;;
    dbt)
        # Check if dbt can connect to warehouse
        dbt debug --target ${DBT_TARGET:-dev} || exit 1
        ;;
    python)
        # Check if Python service is running
        python -c "
import sys
import importlib.util
sys.path.append('/usr/app')
try:
    import src
    print('Service is healthy')
except ImportError as e:
    print(f'Service unhealthy: {e}')
    sys.exit(1)
        " || exit 1
        ;;
    postgres)
        # Check if PostgreSQL is accepting connections
        pg_isready -h ${POSTGRES_HOST:-localhost} -p ${POSTGRES_PORT:-5432} -U ${POSTGRES_USER:-airflow} || exit 1
        ;;
    redis)
        # Check if Redis is responding
        redis-cli -h ${REDIS_HOST:-localhost} -p ${REDIS_PORT:-6379} ping || exit 1
        ;;
    *)
        echo "Unknown service type: ${SERVICE_TYPE}"
        exit 1
        ;;
esac

echo "Health check passed for ${SERVICE_TYPE}"