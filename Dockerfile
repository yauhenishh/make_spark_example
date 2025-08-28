FROM bitnami/spark:3.4-debian-11

# Switch to root to install dependencies
USER root

# Install Python dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3-pip \
    python3-dev \
    gcc \
    g++ && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application source
COPY src/ ./src/
COPY setup.py pyproject.toml ./

# Install the application
RUN pip3 install -e .

# Create reports directory
RUN mkdir -p /app/reports && chown -R 1001:1001 /app

# Switch back to spark user
USER 1001

# Set Python path
ENV PYTHONPATH=/app:$PYTHONPATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Default command - can be overridden
CMD ["python3", "-m", "src.spark_job", "--help"]