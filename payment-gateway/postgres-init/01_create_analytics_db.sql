-- Create the analytics database for dbt mart tables
-- This runs automatically when the PostgreSQL container first starts.
-- The Airflow metadata DB is created via POSTGRES_DB env var.

-- Create dbt_user role (required by dbt-postgres internals even when using dbt-mysql)
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'dbt_user') THEN
    CREATE ROLE dbt_user WITH LOGIN PASSWORD 'dbt_password';
  END IF;
END $$;

-- Create analytics database
SELECT 'CREATE DATABASE analytics'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'analytics'
)\gexec

-- Grant dbt_user access to analytics
GRANT ALL PRIVILEGES ON DATABASE analytics TO dbt_user;
