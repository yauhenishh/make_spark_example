#!/bin/bash
# Test script for Docker Spark setup

echo "Testing Docker Spark Setup..."
echo "==============================="

# Check if docker and docker-compose are installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed"
    exit 1
else
    echo "✓ Docker is installed"
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed"
    exit 1
else
    echo "✓ Docker Compose is installed"
fi

# Check if data files exist
if [ ! -f "data/historical_transactions.parquet" ]; then
    echo "⚠️  Warning: data/historical_transactions.parquet not found"
    echo "   Please add your data files to the data/ directory"
fi

if [ ! -f "data/merchants.csv" ]; then
    echo "⚠️  Warning: data/merchants.csv not found"
    echo "   Please add your data files to the data/ directory"
fi

echo ""
echo "Docker configuration looks good!"
echo ""
echo "To start using Spark in Docker:"
echo "1. make docker-build    # Build images"
echo "2. make docker-up       # Start services"
echo "3. make docker-spark-run # Run analysis"
echo ""
echo "For more information, see DOCKER_SPARK_README.md"