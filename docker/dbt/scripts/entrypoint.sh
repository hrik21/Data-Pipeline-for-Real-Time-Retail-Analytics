#!/bin/bash
set -e

# Set default command if none provided
if [ $# -eq 0 ]; then
    set -- "run"
fi

# Wait for data warehouse connection
echo "Testing data warehouse connection..."
dbt debug --target ${DBT_TARGET:-dev} || {
    echo "Warning: Data warehouse connection failed. Continuing anyway..."
}

# Execute dbt command
case "$1" in
    run)
        echo "Running dbt models..."
        exec dbt run --target ${DBT_TARGET:-dev}
        ;;
    test)
        echo "Running dbt tests..."
        exec dbt test --target ${DBT_TARGET:-dev}
        ;;
    compile)
        echo "Compiling dbt models..."
        exec dbt compile --target ${DBT_TARGET:-dev}
        ;;
    docs)
        echo "Generating dbt documentation..."
        dbt docs generate --target ${DBT_TARGET:-dev}
        exec dbt docs serve --host 0.0.0.0 --port 8080
        ;;
    seed)
        echo "Loading dbt seeds..."
        exec dbt seed --target ${DBT_TARGET:-dev}
        ;;
    debug)
        echo "Running dbt debug..."
        exec dbt debug --target ${DBT_TARGET:-dev}
        ;;
    *)
        echo "Executing custom dbt command: $@"
        exec dbt "$@"
        ;;
esac