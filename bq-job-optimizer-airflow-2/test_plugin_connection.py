#!/usr/bin/env python3
"""
Simple test script to validate the Rabbit BQ Optimizer plugin's connection loading.
This tests the core functionality without requiring a full DAG execution.
"""
import os
import sys
from unittest.mock import MagicMock, patch

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


def test_missing_base_url_uses_default():
    """Ensure that omitting api_base_url falls back to the default endpoint."""
    print("\nTesting default base URL fallback...")

    dummy_connection = MagicMock()
    dummy_connection.password = "test-api-key"
    dummy_connection.extra_dejson = {}

    with patch(
        "rabbit_bq_optimizer_plugin.BaseHook.get_connection",
        return_value=dummy_connection,
    ):
        credentials = _load_rabbit_credentials()

        if credentials["base_url"] is None:
            print("✓ No base URL provided; default endpoint will be used by the client")
            return True

        print("✗ Expected base URL to be None, got:", credentials["base_url"])
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("Rabbit BQ Optimizer Plugin Connection Test")
    print("=" * 60)

    all_tests_passed = True

    conn_exists = test_connection_exists()
    if not conn_exists:
        print("\n" + "=" * 60)
        print("✗ Connection does not exist. Please run setup script first.")
        print("=" * 60)
        sys.exit(1)

    if not test_connection_loading():
        all_tests_passed = False

    if not test_missing_base_url_uses_default():
        all_tests_passed = False

    print("\n" + "=" * 60)
    if all_tests_passed:
        print("✓ All tests passed! Plugin can load credentials from connection.")
        print("=" * 60)
        sys.exit(0)
    else:
        print("✗ One or more tests failed")
        print("=" * 60)
        sys.exit(1)
