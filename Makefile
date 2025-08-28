.PHONY: install install-dev test lint format clean run-example help docker-up docker-down docker-logs docker-build load-data run-analysis spark-run spark-task1 spark-task2 spark-task3 spark-task4 spark-task5 spark-all spark-submit spark-cluster

install:
	uv pip install -r requirements.txt

install-dev:
	uv pip install -r requirements-dev.txt

test:
	pytest tests/ -v --cov=src --cov-report=html --cov-report=term

lint:
	# Run ruff linting
	ruff check src tests
	# Run mypy type checking
	cd . && mypy src/ tests/ --explicit-package-bases

format:
	# Run ruff formatting
	ruff format src tests

lint-fix:
	# Run ruff with auto-fix
	ruff check --fix src tests

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -delete
	rm -rf .coverage htmlcov .pytest_cache .mypy_cache
	rm -rf reports/

run-example:
	@echo "Please provide the paths to your data files:"
	@echo "Example: python -m src.cli -t /path/to/historical_transactions.parquet -m /path/to/merchants.csv run-all"

# Default paths for data files - override with make TRANS_PATH=... MERCH_PATH=...
TRANS_PATH ?= data/historical_transactions.parquet
MERCH_PATH ?= data/merchants.csv
OUTPUT_DIR ?= reports

# Spark configuration
SPARK_MASTER ?= local[4]
SPARK_MEMORY ?= 2g
SPARK_CORES ?= 2

# Run specific Spark tasks
spark-task1:
	python -m src.spark_job -t $(TRANS_PATH) -m $(MERCH_PATH) --task 1 -o $(OUTPUT_DIR)

spark-task2:
	python -m src.spark_job -t $(TRANS_PATH) -m $(MERCH_PATH) --task 2 -o $(OUTPUT_DIR)

spark-task3:
	python -m src.spark_job -t $(TRANS_PATH) -m $(MERCH_PATH) --task 3 -o $(OUTPUT_DIR)

spark-task4:
	python -m src.spark_job -t $(TRANS_PATH) -m $(MERCH_PATH) --task 4 -o $(OUTPUT_DIR)

spark-task5:
	python -m src.spark_job -t $(TRANS_PATH) -m $(MERCH_PATH) --task 5 -o $(OUTPUT_DIR)

# Run all Spark tasks
spark-all:
	python -m src.spark_job -t $(TRANS_PATH) -m $(MERCH_PATH) --task all -o $(OUTPUT_DIR)

# Run with spark-submit
spark-submit:
	spark-submit \
		--master $(SPARK_MASTER) \
		--driver-memory $(SPARK_MEMORY) \
		--executor-memory $(SPARK_MEMORY) \
		--executor-cores $(SPARK_CORES) \
		src/spark_job.py \
		-t $(TRANS_PATH) \
		-m $(MERCH_PATH) \
		--task all \
		-o $(OUTPUT_DIR)

# Run on Spark cluster
spark-cluster:
	./submit_spark_job.sh \
		-t $(TRANS_PATH) \
		-m $(MERCH_PATH) \
		--task all \
		--master $(SPARK_MASTER) \
		--executor-memory $(SPARK_MEMORY) \
		--executor-cores $(SPARK_CORES) \
		-o $(OUTPUT_DIR)

# Interactive Spark shell
spark-shell:
	spark-shell \
		--master $(SPARK_MASTER) \
		--driver-memory $(SPARK_MEMORY) \
		--executor-memory $(SPARK_MEMORY)

# Run without Hive
spark-no-hive:
	python -m src.spark_job -t $(TRANS_PATH) -m $(MERCH_PATH) --task all -o $(OUTPUT_DIR) --no-hive

help:
	@echo "Available commands:"
	@echo "  make install      - Install production dependencies"
	@echo "  make install-dev  - Install development dependencies"
	@echo "  make test        - Run tests with coverage"
	@echo "  make lint        - Run linting checks"
	@echo "  make format      - Format code with Ruff"
	@echo "  make clean       - Clean temporary files"
	@echo "  make run-example - Show example CLI usage"
	@echo ""
	@echo "Spark Job commands:"
	@echo "  make spark-all    - Run all analysis tasks (default: local mode)"
	@echo "  make spark-task1  - Run Task 1: Top merchants by city/month"
	@echo "  make spark-task2  - Run Task 2: Average sales by merchant/state"
	@echo "  make spark-task3  - Run Task 3: Top hours by category"
	@echo "  make spark-task4  - Run Task 4: Popular merchants & location analysis"
	@echo "  make spark-task5  - Run Task 5: Business recommendations"
	@echo "  make spark-submit - Run with spark-submit"
	@echo "  make spark-cluster- Run on Spark cluster"
	@echo "  make spark-no-hive- Run without Hive support"
	@echo "  make spark-shell  - Open interactive Spark shell"
	@echo ""
	@echo "Configuration (override with make VAR=value):"
	@echo "  TRANS_PATH=$(TRANS_PATH)"
	@echo "  MERCH_PATH=$(MERCH_PATH)"
	@echo "  OUTPUT_DIR=$(OUTPUT_DIR)"
	@echo "  SPARK_MASTER=$(SPARK_MASTER)"
	@echo "  SPARK_MEMORY=$(SPARK_MEMORY)"
	@echo "  SPARK_CORES=$(SPARK_CORES)"
	@echo ""
	@echo "Examples:"
	@echo "  make spark-all TRANS_PATH=/data/trans.parquet MERCH_PATH=/data/merchants.csv"
	@echo "  make spark-task1 OUTPUT_DIR=custom_reports"
	@echo "  make spark-cluster SPARK_MASTER=spark://master:7077 SPARK_MEMORY=4g"
	@echo ""
	@echo "Docker commands:"
	@echo "  make docker-build      - Build Docker images"
	@echo "  make docker-up         - Start all Docker services"
	@echo "  make docker-down       - Stop all Docker services"
	@echo "  make docker-logs       - Follow Docker logs"
	@echo "  make docker-clean      - Clean Docker volumes and images"
	@echo "  make docker-spark-run  - Run all Spark tasks in Docker"
	@echo "  make docker-spark-task TASK=N - Run specific task in Docker"
	@echo "  make docker-spark-shell- Open Spark shell in Docker"

# Docker commands
docker-build:
	docker-compose build

docker-up:
	docker-compose up -d
	@echo "Waiting for services to start..."
	@sleep 15
	@echo "Services are starting. Check logs with 'make docker-logs'"
	@echo "Spark UI available at: http://localhost:8080"

docker-down:
	docker-compose down -v

docker-logs:
	docker-compose logs -f

docker-clean:
	docker-compose down -v
	docker system prune -f

# Run Spark job in Docker
docker-spark-run:
	docker-compose run --rm spark-app

# Run specific task in Docker
docker-spark-task:
	@echo "Usage: make docker-spark-task TASK=<task_number>"
	@echo "Running task $(TASK) in Docker..."
	docker-compose run --rm spark-app python -m src.spark_job \
		-t /data/historical_transactions.parquet \
		-m /data/merchants.csv \
		--task $(TASK) \
		-o /app/reports \
		--no-hive

# Interactive Spark shell in Docker
docker-spark-shell:
	docker-compose run --rm spark-app spark-shell \
		--master spark://spark-master:7077 \
		--conf spark.sql.catalogImplementation=in-memory