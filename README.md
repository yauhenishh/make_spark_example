# Billups Data Analysis Project

A PySpark-based data analysis solution for processing merchant transaction data and generating business insights, with integrated Hive metastore support and Apache Superset for visualization.

## Overview

This project implements a comprehensive data analysis pipeline using PySpark to analyze historical merchant transactions. It addresses five key business questions:

1. Top 5 merchants by purchase amount for each month/city
2. Average sale amounts by merchant and state
3. Peak business hours by product category
4. Merchant popularity analysis by location
5. Business recommendations for new merchants

## Project Structure

```
billups-data-analysis/
├── src/
│   ├── data/          # Data loading and cleaning modules
│   ├── analysis/      # Analysis tasks implementation
│   ├── utils/         # Utility functions
│   └── cli.py         # Command-line interface
├── tests/             # Unit tests
├── notebooks/         # Jupyter notebooks for exploration
├── reports/           # Output reports
├── requirements.txt   # Production dependencies
└── README.md          # This file
```

## Installation

### Prerequisites

- Python 3.10+
- Java 11 (required for PySpark)
- Docker and Docker Compose (for full stack deployment)
- uv (Python package manager)

### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd billups-data-analysis
```

2. Install uv package manager:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

3. Install dependencies:
```bash
uv pip install -r requirements.txt
```

4. For development:
```bash
uv pip install -r requirements-dev.txt
```

## Usage

### Command Line Interface

The project provides a CLI for running analysis tasks:

```bash
# Run all tasks with Hive support
python -m src.cli -t /path/to/historical_transactions.parquet -m /path/to/merchants.csv run-all

# Run without Hive (local mode)
python -m src.cli -t /path/to/historical_transactions.parquet -m /path/to/merchants.csv --no-hive run-all

# Load raw data into Hive tables
python -m src.cli -t /path/to/historical_transactions.parquet -m /path/to/merchants.csv load-raw-data

# Run individual tasks
python -m src.cli -t /path/to/historical_transactions.parquet -m /path/to/merchants.csv task1
python -m src.cli -t /path/to/historical_transactions.parquet -m /path/to/merchants.csv task2
python -m src.cli -t /path/to/historical_transactions.parquet -m /path/to/merchants.csv task3
python -m src.cli -t /path/to/historical_transactions.parquet -m /path/to/merchants.csv task4
python -m src.cli -t /path/to/historical_transactions.parquet -m /path/to/merchants.csv task5
```

### Options

- `-t, --transactions`: Path to historical_transactions.parquet file (required)
- `-m, --merchants`: Path to merchants.csv file (required)
- `-o, --output`: Custom output path for results (optional)

### Example

```bash
# Generate top merchants report
python -m src.cli \
  -t data/historical_transactions.parquet \
  -m data/merchants.csv \
  task1 \
  --output reports/custom_top_merchants.csv
```

## Data Requirements

### Input Files

1. **historical_transactions.parquet**: Transaction records with columns:
   - merchant_id
   - purchase_amount
   - purchase_date
   - category
   - installments

2. **merchants.csv**: Merchant information with columns:
   - merchant_id
   - merchant_name
   - city_id
   - state_id

### Data Cleaning Rules

1. Missing merchant names are replaced with merchant_id
2. Null categories are replaced with "Unknown category"
3. Transactions without matching merchants use merchant_id as merchant name

## Analysis Tasks

### Task 1: Top Merchants by City/Month
Identifies the top 5 merchants by total purchase amount for each city and month combination.

### Task 2: Average Sales by State
Calculates average transaction amounts for each merchant in each state.

### Task 3: Peak Business Hours
Finds the top 3 hours with highest sales for each product category.

### Task 4: Location Analysis
Analyzes merchant popularity by city and correlation between locations and product categories.

### Task 5: Business Recommendations
Provides data-driven recommendations for new merchants including:
- Best cities to operate in
- Most profitable product categories
- Seasonal trends
- Optimal operating hours
- Installment payment analysis

## Development

### Running Tests

```bash
# Run all tests
make test

# Run with coverage
pytest tests/ -v --cov=src

# Run specific test file
pytest tests/test_data_loader.py -v
```

### Code Quality

```bash
# Format code
make format

# Run linters
make lint

# Clean temporary files
make clean
```

### Code Style

- Ruff for formatting and linting (line length: 100)
- MyPy for type checking
- Python 3.10+ with F alias for PySpark functions

## Performance Optimization

The solution implements several PySpark optimizations:
- Adaptive Query Execution (AQE) enabled
- Coalesce partitions for efficient writes
- Window functions for ranking operations
- Broadcast joins for small dimension tables

## Scalability Considerations

For production deployment with larger datasets:

1. **Partitioning Strategy**: Partition historical data by year/month for efficient queries
2. **Caching**: Cache frequently accessed DataFrames in memory
3. **Storage Format**: Use Parquet with proper compression (Snappy/LZ4)
4. **Cluster Configuration**: Tune executor memory and cores based on data volume

## Architecture Recommendations

For daily processing pipeline:
1. **Apache Airflow** for workflow orchestration
2. **Delta Lake** for ACID transactions and time travel
3. **AWS S3/Azure Data Lake** for data storage
4. **Databricks/EMR** for managed Spark clusters
5. **Data quality checks** using Great Expectations

## Docker Deployment with Full Stack

The project includes a complete data analytics stack with Spark, Hive, and Superset:

### Starting the Stack

```bash
# Place your data files in the data/ directory
mkdir data
cp Historical_transactions.parquet data/
cp Merchants.csv data/

# Start all services
docker-compose up -d

# Wait for services to initialize (about 2-3 minutes)
docker-compose logs -f
```

### Services

1. **Spark Cluster**: 
   - Master UI: http://localhost:8080
   - Submit jobs via spark-submit

2. **Hive Metastore**: 
   - Thrift port: 9083
   - Stores metadata for all tables

3. **Apache Superset**: 
   - UI: http://localhost:8088
   - Default login: admin/admin
   - Connect to Hive tables for visualization

4. **PostgreSQL**: 
   - Stores metadata for Hive and Superset
   - Port: 5432

### Running Analysis in Docker

```bash
# Load data into Hive
docker-compose run billups-app load-raw-data

# Run all analysis tasks
docker-compose run billups-app run-all
```

### Accessing Data in Superset

1. Navigate to http://localhost:8088
2. Login with admin/admin
3. Go to Data → Databases → Add Database
4. Add Hive connection:
   ```
   hive://hive@hive-metastore:10000/billups_analytics
   ```
5. Explore tables and create dashboards

## Hive Tables Created

The analysis creates the following tables in the `billups_analytics` database:

### Raw Data Tables
- `raw_transactions` - Original transaction data (partitioned by year/month)
- `raw_merchants` - Original merchant data
- `cleaned_transactions` - Cleaned and joined data (partitioned by year/month)

### Analysis Result Tables
- `top_merchants_by_month_city` - Task 1 results
- `avg_sales_by_merchant_state` - Task 2 results
- `top_hours_by_category` - Task 3 results
- `popular_merchants_by_city` - Task 4 results (merchants)
- `city_dominant_categories` - Task 4 results (categories)
- `recommendation_top_cities` - Task 5 cities recommendation
- `recommendation_top_categories` - Task 5 categories recommendation
- `monthly_sales_trends` - Task 5 monthly trends
- `hourly_sales_patterns` - Task 5 hourly patterns
- `installment_profitability_analysis` - Task 5 installment analysis

## License

This project is part of the Billups Data Engineering Challenge.

## Contact

For questions or issues, please contact the development team.