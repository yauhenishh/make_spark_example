# Running Spark in Docker

This guide explains how to run the Spark data analysis application using Docker.

## Prerequisites

- Docker and Docker Compose installed
- Data files in the `data/` directory:
  - `historical_transactions.parquet`
  - `merchants.csv`

## Quick Start

1. Build the Docker images:
   ```bash
   make docker-build
   ```

2. Start all services:
   ```bash
   make docker-up
   ```

3. Run all analysis tasks:
   ```bash
   make docker-spark-run
   ```

## Available Commands

### Basic Docker Operations
- `make docker-build` - Build Docker images
- `make docker-up` - Start all services (Spark Master, Worker, Hive Metastore, PostgreSQL)
- `make docker-down` - Stop all services and remove volumes
- `make docker-logs` - Follow Docker logs
- `make docker-clean` - Clean up Docker volumes and unused images

### Running Spark Jobs
- `make docker-spark-run` - Run all analysis tasks
- `make docker-spark-task TASK=1` - Run a specific task (1-5)
- `make docker-spark-shell` - Open an interactive Spark shell

## Architecture

The Docker setup includes:
- **Spark Master**: Coordinates job execution (UI at http://localhost:8080)
- **Spark Worker**: Executes tasks
- **PostgreSQL**: Stores Hive metastore metadata
- **Hive Metastore**: Manages metadata for structured data
- **Spark App**: Your analysis application

## Monitoring

- Spark Master UI: http://localhost:8080
- Spark Application UI: http://localhost:4040 (when a job is running)

## Configuration

The Spark configuration can be modified in:
- `conf/spark-defaults.conf` - Spark configuration
- `docker-compose.yml` - Container settings and resources

## Troubleshooting

1. If services fail to start, check logs:
   ```bash
   make docker-logs
   ```

2. To restart with fresh state:
   ```bash
   make docker-clean
   make docker-up
   ```

3. For memory issues, adjust worker memory in `docker-compose.yml`:
   ```yaml
   SPARK_WORKER_MEMORY=4g  # Increase as needed
   ```

## Running Without Hive

The application runs with `--no-hive` flag by default in Docker, which means results are displayed in logs rather than written to Hive tables.