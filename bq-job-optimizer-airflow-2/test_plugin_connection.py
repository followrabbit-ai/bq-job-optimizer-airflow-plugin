#!/usr/bin/env python3
"""
Simple test script to validate the Rabbit BQ Optimizer plugin's connection loading.
This tests the core functionality without requiring a full DAG execution.
"""
import os
import sys

# Set up Airflow environment
# When running from the repo root, airflow_home is in the parent directory
script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(script_dir)
os.environ["AIRFLOW_HOME"] = os.path.join(repo_root, "airflow_home")
sys.path.insert(0, os.path.join(os.environ["AIRFLOW_HOME"], "plugins"))

# Import Airflow components
from airflow.exceptions import AirflowException  # noqa: E402
from airflow.hooks.base import BaseHook  # noqa: E402

# Import the plugin's connection loading function
from rabbit_bq_optimizer_plugin import (  # noqa: E402
    RABBIT_API_CONN_ID,
    _load_rabbit_credentials,
)


def test_connection_loading():
    """Test that the plugin can load credentials from the Airflow connection."""
    print("Testing Rabbit API connection loading...")

    try:
        credentials = _load_rabbit_credentials()
        print(f"✓ Successfully loaded credentials from connection '{RABBIT_API_CONN_ID}'")
        masked_key = "*" * len(credentials["api_key"]) if credentials["api_key"] else "MISSING"
        print(f"  - API Key: {masked_key}")
        print(f"  - Base URL: {credentials['base_url']}")
        return True
    except RuntimeError as e:
        print(f"✗ Failed to load connection: {e}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_connection_exists():
    """Test that the connection exists."""
    print(f"\nChecking if connection '{RABBIT_API_CONN_ID}' exists...")

    try:
        connection = BaseHook.get_connection(RABBIT_API_CONN_ID)
        print("✓ Connection found")
        print(f"  - Type: {connection.conn_type}")
        print(f"  - Has password: {'Yes' if connection.password else 'No'}")
        print(f"  - Extra: {connection.extra_dejson if connection.extra_dejson else 'None'}")
        return True
    except AirflowException as e:
        print(f"✗ Connection not found: {e}")
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("Rabbit BQ Optimizer Plugin Connection Test")
    print("=" * 60)

    # Test connection exists
    conn_exists = test_connection_exists()

    # Test loading credentials
    if conn_exists:
        creds_loaded = test_connection_loading()

        if creds_loaded:
            print("\n" + "=" * 60)
            print("✓ All tests passed! Plugin can load credentials from connection.")
            print("=" * 60)
            sys.exit(0)
        else:
            print("\n" + "=" * 60)
            print("✗ Connection loading failed")
            print("=" * 60)
            sys.exit(1)
    else:
        print("\n" + "=" * 60)
        print("✗ Connection does not exist. Please run setup script first.")
        print("=" * 60)
        sys.exit(1)
