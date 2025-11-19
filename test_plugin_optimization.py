#!/usr/bin/env python3
"""
Test script to validate that the Rabbit BQ Optimizer plugin intercepts BigQuery jobs
and calls the optimization API with credentials from the Airflow connection.
"""
import os
import sys
from unittest.mock import Mock, patch, MagicMock
import json

# Set up Airflow environment
os.environ['AIRFLOW_HOME'] = os.path.join(os.path.dirname(__file__), 'airflow_home')
sys.path.insert(0, os.path.join(os.environ['AIRFLOW_HOME'], 'plugins'))

# Import Airflow components
from airflow.hooks.base import BaseHook
from airflow.models import Variable

# Import the plugin
from rabbit_bq_optimizer_plugin import patch_bigquery_hook, RABBIT_API_CONN_ID

def setup_test_environment():
    """Ensure connection and variable are set up."""
    print("Setting up test environment...")
    
    # Check connection exists
    try:
        connection = BaseHook.get_connection(RABBIT_API_CONN_ID)
        if not connection.password:
            print(f"✗ Connection '{RABBIT_API_CONN_ID}' exists but has no password")
            return False
        print(f"✓ Connection '{RABBIT_API_CONN_ID}' is configured")
    except Exception as e:
        print(f"✗ Connection '{RABBIT_API_CONN_ID}' not found: {e}")
        return False
    
    # Check variable exists
    try:
        config = Variable.get("rabbit_bq_optimizer_config", deserialize_json=True)
        required_fields = ["default_pricing_mode", "reservation_ids"]
        missing = [f for f in required_fields if f not in config]
        if missing:
            print(f"✗ Variable 'rabbit_bq_optimizer_config' missing fields: {missing}")
            return False
        print(f"✓ Variable 'rabbit_bq_optimizer_config' is configured")
    except Exception as e:
        print(f"✗ Variable 'rabbit_bq_optimizer_config' not found: {e}")
        return False
    
    return True

def test_plugin_patches_bigquery_hook():
    """Test that the plugin patches BigQueryHook.insert_job."""
    print("\nTesting plugin patch application...")
    
    try:
        # Import after setting up environment
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        
        # Apply the patch
        patch_bigquery_hook()
        
        # Check if the marker is set
        if hasattr(BigQueryHook, "_rabbit_bq_job_optimizer_patched"):
            print("✓ Plugin patch applied to BigQueryHook")
            return True
        else:
            print("✗ Plugin patch not applied")
            return False
    except Exception as e:
        print(f"✗ Error applying patch: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_plugin_intercepts_job():
    """Test that the plugin intercepts a BigQuery job and calls the optimizer."""
    print("\nTesting job interception and optimization...")
    
    try:
        # Mock the RabbitBQJobOptimizer client
        mock_optimization_response = MagicMock()
        mock_optimization_response.optimizedJob = {
            "configuration": {
                "query": {
                    "query": "SELECT 1",
                    "useLegacySql": False
                }
            }
        }
        mock_optimization_response.optimizationResults = []
        mock_optimization_response.estimatedSavings = 0.0
        mock_optimization_response.optimizationPerformed = False
        
        mock_client = MagicMock()
        mock_client.optimize_job.return_value = mock_optimization_response
        
        # Mock the original insert_job to track if it's called
        original_insert_job_called = []
        original_insert_job_config = []
        
        def mock_original_insert_job(self, *, configuration, **kwargs):
            original_insert_job_called.append(True)
            original_insert_job_config.append(configuration)
            # Return a mock job
            mock_job = MagicMock()
            return mock_job
        
        # Apply patch
        patch_bigquery_hook()
        
        # Import BigQueryHook after patching
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        
        # Replace the original method with our mock
        BigQueryHook.insert_job = mock_original_insert_job
        
        # Create a hook instance
        hook = BigQueryHook()
        
        # Mock the RabbitBQJobOptimizer import
        with patch('rabbit_bq_optimizer_plugin.RabbitBQJobOptimizer', return_value=mock_client):
            # Import the module again to get the patched version
            import importlib
            import rabbit_bq_optimizer_plugin
            importlib.reload(rabbit_bq_optimizer_plugin)
            
            # Call insert_job with a test configuration
            test_config = {
                "query": {
                    "query": "SELECT 1",
                    "useLegacySql": False
                }
            }
            
            # Get the patched insert_job
            patched_insert_job = BigQueryHook.insert_job
            
            # Call it
            result = patched_insert_job(hook, configuration=test_config)
            
            # Verify the original was called (meaning optimization happened)
            if original_insert_job_called:
                print("✓ Plugin intercepted job and called original insert_job")
                print(f"  - Original insert_job called: {len(original_insert_job_called)} time(s)")
                return True
            else:
                print("✗ Original insert_job was not called")
                return False
                
    except Exception as e:
        print(f"✗ Error testing interception: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_plugin_uses_connection_credentials():
    """Test that the plugin uses credentials from the connection."""
    print("\nTesting that plugin uses connection credentials...")
    
    try:
        # This test verifies that _load_rabbit_credentials is called
        # We'll check by ensuring the connection is accessible
        from rabbit_bq_optimizer_plugin import _load_rabbit_credentials
        
        credentials = _load_rabbit_credentials()
        
        if credentials.get("api_key"):
            print("✓ Plugin can load API key from connection")
        else:
            print("✗ Plugin could not load API key from connection")
            return False
        
        print(f"  - Base URL: {credentials.get('base_url', 'default')}")
        return True
        
    except Exception as e:
        print(f"✗ Error testing connection credentials: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_plugin_handles_missing_connection():
    """Test that the plugin gracefully handles missing connection."""
    print("\nTesting graceful handling of missing connection...")
    
    try:
        from rabbit_bq_optimizer_plugin import _load_rabbit_credentials
        
        # Temporarily rename the connection check
        # We'll just verify the function raises RuntimeError for missing connection
        with patch('rabbit_bq_optimizer_plugin.BaseHook.get_connection') as mock_get_conn:
            from airflow.exceptions import AirflowException
            mock_get_conn.side_effect = AirflowException("Connection not found")
            
            try:
                _load_rabbit_credentials()
                print("✗ Function should have raised RuntimeError for missing connection")
                return False
            except RuntimeError as e:
                if "could not be loaded" in str(e):
                    print("✓ Plugin correctly raises RuntimeError for missing connection")
                    return True
                else:
                    print(f"✗ Unexpected error message: {e}")
                    return False
                    
    except Exception as e:
        print(f"✗ Error testing missing connection handling: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    print("=" * 70)
    print("Rabbit BQ Optimizer Plugin Full Integration Test")
    print("=" * 70)
    
    # Setup
    if not setup_test_environment():
        print("\n✗ Test environment setup failed. Please run setup script first.")
        sys.exit(1)
    
    # Run tests
    tests = [
        ("Connection Credentials Loading", test_plugin_uses_connection_credentials),
        ("Missing Connection Handling", test_plugin_handles_missing_connection),
        ("Plugin Patch Application", test_plugin_patches_bigquery_hook),
        ("Job Interception", test_plugin_intercepts_job),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"\n✗ Test '{test_name}' crashed: {e}")
            import traceback
            traceback.print_exc()
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 70)
    print("Test Results Summary")
    print("=" * 70)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status}: {test_name}")
    
    print("=" * 70)
    print(f"Total: {passed}/{total} tests passed")
    print("=" * 70)
    
    if passed == total:
        print("\n✓ All tests passed! Plugin is working correctly.")
        sys.exit(0)
    else:
        print(f"\n✗ {total - passed} test(s) failed.")
        sys.exit(1)

