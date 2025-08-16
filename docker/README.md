# Docker Setup for Real-time Data Pipeline

This directory contains Docker configurations for the real-time data pipeline project.

## Architecture

The containerized setup includes:

- **Airflow**: Orchestration engine with webserver, scheduler, and worker containers
- **dbt**: Data transformation service
- **Python Services**: Custom ingestion, processing, and monitoring services
- **PostgreSQL**: Metadata database for Airflow
- **Redis**: Message broker for Airflow Celery executor

## Quick Start

1. **Setup environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

2. **Build and start services**:
   ```bash
   make build
   make up
   ```

3. **Access services**:
   - Airflow UI: http://localhost:8080 (admin/admin)
   - Monitoring: http://localhost:9090

## Directory Structure

```
docker/
├── airflow/
│   ├── Dockerfile          # Multi-stage Airflow image
│   └── scripts/
│       └── entrypoint.sh   # Airflow startup script
├── dbt/
│   ├── Dockerfile          # dbt service image
│   └── scripts/
│       └── entrypoint.sh   # dbt startup script
├── python/
│   ├── Dockerfile          # Python services image
│   └── scripts/
│       └── entrypoint.sh   # Python services startup script
├── postgres/
│   └── init/
│       └── 01-init-databases.sql  # Database initialization
└── scripts/
    └── health-check.sh     # Health check utilities
```

## Services

### Airflow Services

- **airflow-webserver**: Web UI and API server
- **airflow-scheduler**: DAG scheduler and task dispatcher
- **airflow-worker**: Celery worker for task execution

### Data Services

- **dbt**: Data transformation service
- **ingestion-service**: Data ingestion from sources
- **processing-service**: Data processing and validation
- **monitoring-service**: Pipeline monitoring and metrics

### Infrastructure Services

- **postgres**: Airflow metadata database
- **redis**: Celery message broker

## Configuration

### Environment Variables

Key environment variables (see `.env.example`):

- `ENVIRONMENT`: deployment environment (development/production)
- `POSTGRES_*`: PostgreSQL connection settings
- `AIRFLOW_*`: Airflow configuration
- `DBT_*`: dbt and Snowflake settings

### Volume Mounts

- `./airflow/dags`: Airflow DAG files
- `./airflow/logs`: Airflow execution logs
- `./dbt`: dbt project files
- `./src`: Python source code
- `./config`: Configuration files

## Development

### Local Development

```bash
# Start development environment
make up

# View logs
make logs

# Check service health
make health

# Run only specific services
make dbt      # dbt services only
make airflow  # Airflow services only
```

### Testing

```bash
# Run health checks
make test

# Check individual services
docker-compose exec airflow-webserver curl -f http://localhost:8080/health
docker-compose exec dbt dbt debug
```

## Production Deployment

```bash
# Deploy to production
make prod-deploy

# Or manually
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

## Troubleshooting

### Common Issues

1. **Port conflicts**: Ensure ports 8080, 5432, 6379, 9090 are available
2. **Permission issues**: Check file permissions for mounted volumes
3. **Memory issues**: Increase Docker memory allocation for production

### Health Checks

All services include health checks:

```bash
# Check all services
docker-compose ps

# Individual health checks
docker-compose exec postgres pg_isready -U airflow
docker-compose exec redis redis-cli ping
docker-compose exec airflow-webserver curl -f http://localhost:8080/health
```

### Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f airflow-scheduler
docker-compose logs -f dbt
```

## Scaling

### Horizontal Scaling

Scale worker services:

```bash
# Scale Airflow workers
docker-compose up -d --scale airflow-worker=3

# Scale processing services
docker-compose up -d --scale processing-service=2
```

### Resource Limits

Production deployment includes resource limits:

- Airflow webserver: 2GB RAM, 1 CPU
- Airflow workers: 4GB RAM, 2 CPU
- dbt service: 1GB RAM, 0.5 CPU
- Processing services: 2GB RAM, 1 CPU

## Security

### Best Practices

1. **Non-root users**: All services run as non-root users
2. **Secrets management**: Use environment variables for sensitive data
3. **Network isolation**: Services communicate through dedicated network
4. **Image scanning**: Regular security scans of base images

### Production Security

- Use external secret management (AWS Secrets Manager, etc.)
- Enable TLS for all external communications
- Regular security updates for base images
- Network policies for container isolation