#!/bin/bash
# Script to run tests with proper Python path setup

export PYTHONPATH="${PYTHONPATH}:$(pwd)"
echo "PYTHONPATH set to: $PYTHONPATH"
echo "Current directory: $(pwd)"
echo "Python version: $(python --version)"

# Run tests
python -m pytest tests/ -v --tb=short