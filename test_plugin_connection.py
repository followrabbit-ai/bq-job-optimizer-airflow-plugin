#!/usr/bin/env python3
"""
Simple test script to validate the Rabbit BQ Optimizer plugin's connection loading.
This tests the core functionality without requiring a full DAG execution.
"""
import os
import sys

# Set up Airflow environment
os.environ['AIRFLOW_HOME'] = os.path.join(os.path.dirname(__file__), 'airflow_home')
sys.path.insert(0, os.path.join(os.environ['AIRFLOW_HOME'], 'plugins'))

# Import Airflow components
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException

# Import the plugin's connection loading function
from rabbit_bq_optimizer_plugin import _load_rabbit_credentials, RABBIT_API_CONN_ID

def test_connection_loading():
    """Test that the plugin can load credentials from the Airflow connection."""
    print("Testing Rabbit API connection loading...")
    
    try:
        credentials = _load_rabbit_credentials()
        print(f"✓ Successfully loaded credentials from connection '{RABBIT_API_CONN_ID}'")
        print(f"  - API Key: {'*' * len(credentials['api_key']) if credentials['api_key'] else 'MISSING'}")
        print(f"  - Base URL: {credentials['base_url'] or 'Not set (will use default)'}")
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
        print(f"✓ Connection found")
        print(f"  - Type: {connection.conn_type}")
        print(f"  - Has password: {'Yes' if connection.password else 'No'}")
        print(f"  - Extra: {connection.extra_dejson if connection.extra_dejson else 'None'}")
        return True
    except AirflowException as e:
        print(f"✗ Connection not found: {e}")
        return False

if __name__ == '__main__':
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

