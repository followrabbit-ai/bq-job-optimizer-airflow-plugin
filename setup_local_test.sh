#!/bin/bash
set -e

# Configuration
AIRFLOW_HOME="$(pwd)/airflow_home"
export AIRFLOW_HOME
PLUGIN_DIR="$AIRFLOW_HOME/plugins"
DAGS_DIR="$AIRFLOW_HOME/dags"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Cleanup function
cleanup() {
    echo -e "${YELLOW}Cleaning up local test environment...${NC}"
    
    if [ -d "venv" ]; then
        echo "Removing virtual environment..."
        rm -rf venv
    fi
    
    if [ -d "$AIRFLOW_HOME" ]; then
        echo "Removing Airflow home directory..."
        rm -rf "$AIRFLOW_HOME"
    fi
    
    echo -e "${GREEN}Cleanup complete!${NC}"
    exit 0
}

# Check for cleanup flag
if [ "$1" == "--clean" ] || [ "$1" == "-c" ] || [ "$1" == "clean" ]; then
    cleanup
fi

# Show help
if [ "$1" == "--help" ] || [ "$1" == "-h" ] || [ "$1" == "help" ]; then
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --clean, -c, clean    Clean up venv and airflow_home directories"
    echo "  --help, -h, help      Show this help message"
    echo ""
    echo "This script sets up a local Airflow environment for testing the Rabbit BQ Optimizer plugin."
    exit 0
fi

echo -e "${GREEN}Setting up local Airflow environment in $AIRFLOW_HOME...${NC}"

# Create directories
mkdir -p "$PLUGIN_DIR"
mkdir -p "$DAGS_DIR"

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    # Airflow 2.9.1 requires Python 3.8-3.12
    # Try to use python3.12, python3.11, python3.10, python3.9, or python3.8
    PYTHON_CMD=""
    for py in python3.12 python3.11 python3.10 python3.9 python3.8 python3; do
        if command -v $py >/dev/null 2>&1; then
            PYTHON_VERSION=$($py --version 2>&1 | grep -oE '[0-9]+\.[0-9]+' | head -1)
            MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
            MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)
            if [ "$MAJOR" -eq 3 ] && [ "$MINOR" -ge 8 ] && [ "$MINOR" -le 12 ]; then
                PYTHON_CMD=$py
                echo "Using $PYTHON_CMD (version $PYTHON_VERSION)"
                break
            fi
        fi
    done
    
    if [ -z "$PYTHON_CMD" ]; then
        echo "ERROR: No compatible Python version found (requires Python 3.8-3.12)"
        echo "Please install Python 3.8-3.12 and try again"
        exit 1
    fi
    
    $PYTHON_CMD -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install dependencies with Airflow constraint files
echo "Installing dependencies..."
AIRFLOW_VERSION="2.9.1"
PY_VERSION=$(python -c "import sys; print(f\"{sys.version_info[0]}.{sys.version_info[1]}\")")
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY_VERSION}.txt"

echo "Using constraint file: $CONSTRAINT_URL"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install -r bq-job-optimizer-airflow-2/test_requirements.txt --constraint "${CONSTRAINT_URL}"
pip install --constraint "${CONSTRAINT_URL}" \
    google-auth-httplib2 \
    google-api-python-client \
    pandas-gbq \
    gcloud-aio-bigquery \
    gcloud-aio-storage \
    google-cloud-secret-manager \
    google-cloud-storage

# Initialize Airflow DB
if [ ! -f "$AIRFLOW_HOME/airflow.db" ]; then
    echo "Initializing Airflow database..."
    airflow db init
    
    echo "Creating admin user..."
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin
fi

# Copy plugin and test DAG
echo "Deploying plugin..."
cp bq-job-optimizer-airflow-2/rabbit_bq_optimizer_plugin.py "$PLUGIN_DIR/"

echo "Deploying test DAG..."
cp bq-job-optimizer-airflow-2/test_dag.py "$DAGS_DIR/"

# Setup configuration
echo "Configuring Airflow..."

# 1. Create Connection (using a dummy key for testing)
if ! airflow connections get rabbit_api > /dev/null 2>&1; then
    echo "Creating rabbit_api connection..."
    airflow connections add rabbit_api \
        --conn-type generic \
        --conn-password "dummy-test-key" \
        --conn-extra '{"api_base_url": "https://api.followrabbit.ai/bq-job-optimizer"}'
fi

# 2. Create Variable
echo "Setting rabbit_bq_optimizer_config variable..."
airflow variables set rabbit_bq_optimizer_config '{
    "default_pricing_mode": "on_demand",
    "reservation_ids": [
        "project:us-central1.test-reservation"
    ]
}'

echo -e "${GREEN}Setup complete!${NC}"
echo "To run the test DAG:"
echo "1. Ensure you have a test DAG in $DAGS_DIR"
echo "2. Run: airflow dags test rabbit_optimizer_test $(date +%Y-%m-%d)"
