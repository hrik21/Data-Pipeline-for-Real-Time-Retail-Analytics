-- Initialize databases for the data pipeline

-- Create additional databases if needed
CREATE DATABASE IF NOT EXISTS pipeline_metadata;
CREATE DATABASE IF NOT EXISTS data_quality;

-- Create users with appropriate permissions
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'pipeline_user') THEN
        CREATE ROLE pipeline_user LOGIN PASSWORD 'pipeline_password';
    END IF;
END
$$;

-- Grant permissions
GRANT CONNECT ON DATABASE airflow TO pipeline_user;
GRANT CONNECT ON DATABASE pipeline_metadata TO pipeline_user;
GRANT CONNECT ON DATABASE data_quality TO pipeline_user;

-- Create schemas
\c airflow;
CREATE SCHEMA IF NOT EXISTS pipeline;
GRANT USAGE ON SCHEMA pipeline TO pipeline_user;
GRANT CREATE ON SCHEMA pipeline TO pipeline_user;

\c pipeline_metadata;
CREATE SCHEMA IF NOT EXISTS metadata;
GRANT USAGE ON SCHEMA metadata TO pipeline_user;
GRANT CREATE ON SCHEMA metadata TO pipeline_user;

\c data_quality;
CREATE SCHEMA IF NOT EXISTS quality;
GRANT USAGE ON SCHEMA quality TO pipeline_user;
GRANT CREATE ON SCHEMA quality TO pipeline_user;