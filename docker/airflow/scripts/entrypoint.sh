#!/bin/bash
set -e

# Wait for database to be ready
echo "Waiting for database connection..."
while ! nc -z ${POSTGRES_HOST:-postgres} ${POSTGRES_PORT:-5432}; do
  sleep 1
done
echo "Database is ready!"

# Initialize Airflow database if needed
if [[ "${AIRFLOW_ROLE}" == "webserver" ]]; then
    echo "Initializing Airflow database..."
    airflow db init
    
    # Create admin user if it doesn't exist
    airflow users create \
        --username ${AIRFLOW_ADMIN_USER:-admin} \
        --firstname ${AIRFLOW_ADMIN_FIRSTNAME:-Admin} \
        --lastname ${AIRFLOW_ADMIN_LASTNAME:-User} \
        --role Admin \
        --email ${AIRFLOW_ADMIN_EMAIL:-admin@example.com} \
        --password ${AIRFLOW_ADMIN_PASSWORD:-admin} || true
fi

# Upgrade database schema
if [[ "${AIRFLOW_ROLE}" == "webserver" ]] || [[ "${AIRFLOW_ROLE}" == "scheduler" ]]; then
    echo "Upgrading Airflow database..."
    airflow db upgrade
fi

# Start the appropriate Airflow component
case "${AIRFLOW_ROLE}" in
    webserver)
        echo "Starting Airflow webserver..."
        exec airflow webserver
        ;;
    scheduler)
        echo "Starting Airflow scheduler..."
        exec airflow scheduler
        ;;
    worker)
        echo "Starting Airflow worker..."
        exec airflow celery worker
        ;;
    flower)
        echo "Starting Airflow flower..."
        exec airflow celery flower
        ;;
    *)
        echo "Unknown AIRFLOW_ROLE: ${AIRFLOW_ROLE}"
        exit 1
        ;;
esac