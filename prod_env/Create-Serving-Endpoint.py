import mlflow
from mlflow.deployments import get_deploy_client
from mlflow.tracking import MlflowClient
import os
import time

print("🚀 मॉडल सर्विंग एंडपॉइंट बनाने की प्रक्रिया शुरू हो रही है।")

# Unity Catalog के लिए MLflow registry URI सेट करें
try:
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        mlflow.set_registry_uri("databricks-uc")
        print("✅ MLflow रजिस्ट्री URI को यूनिटी कैटलॉग पर सेट किया गया।")
    else:
        print("⚠️ Databricks वातावरण में नहीं चल रहा है। URI सेट करना छोड़ दिया गया है।")
        
    deploy_client = get_deploy_client("databricks")
    mlflow_client = MlflowClient()
    print("✅ MLflow clients successfully initialized।")
except Exception as e:
    print(f"❌ MLflow क्लाइंट इनिशियलाइज़ेशन विफल हुआ: {e}")
    exit()

# Configuration
model_name = "workspace.ml.house_price_model"
production_alias = "Production"
endpoint_name = "house-price-endpoint"

# Get model version
try:
    prod_version = mlflow_client.get_model_version_by_alias(model_name, production_alias)
    version_number = prod_version.version
    print(f"ℹ️ Model version v{version_number} found with '{production_alias}' alias।")
except Exception as e:
    print(f"❌ Model version retrieval failed: {e}")
    exit()

# Simplified endpoint configuration
endpoint_config = {
    "served_models": [
        {
            "name": f"house_price_model_v{version_number}",
            "model_name": model_name,
            "model_version": str(version_number),
            "workload_size": "Small",
            "scale_to_zero_enabled": True
        }
    ]
}

# Create or update endpoint with reduced waiting
try:
    print(f"ℹ️ Processing endpoint '{endpoint_name}'...")
    
    # Check if endpoint exists
    endpoint_exists = False
    try:
        existing_endpoint = deploy_client.get_endpoint(endpoint_name)
        endpoint_exists = True
        print(f"ℹ️ Endpoint exists. Updating...")
        deploy_client.update_endpoint(endpoint=endpoint_name, config=endpoint_config)
    except Exception:
        print(f"ℹ️ Creating new endpoint...")
        deploy_client.create_endpoint(name=endpoint_name, config=endpoint_config)
    
    # Reduced waiting time with faster checks
    print(f"⏳ Waiting for endpoint deployment (max 3 minutes)...")
    start_time = time.time()
    max_wait_time = 180  # 3 minutes instead of 10
    check_interval = 10   # 10 seconds instead of 15
    last_status = None
    
    for attempt in range(max_wait_time // check_interval):
        try:
            endpoint_status = deploy_client.get_endpoint(endpoint_name)
            
            # Simplified status checking
            if isinstance(endpoint_status, dict):
                ready_status = endpoint_status.get("state", {}).get("ready")
                config_status = endpoint_status.get("state", {}).get("config_update")
                
                current_status = ready_status or config_status or "UNKNOWN"
                
                # Only print if status changed
                if current_status != last_status:
                    print(f"⏳ Status: {current_status}")
                    last_status = current_status
                
                # Check for ready state
                if ready_status == "READY" or ready_status is True:
                    print(f"✅ Endpoint '{endpoint_name}' is ready!")
                    
                    # Try to get prediction URL
                    pred_url = endpoint_status.get("prediction_url")
                    if pred_url:
                        print(f"🌐 Endpoint URL: {pred_url}")
                    
                    print("🏁 Endpoint deployment completed successfully.")
                    exit()
                
                # Check for obvious failures
                if config_status == "UPDATE_FAILED" or ready_status == "FAILED":
                    print(f"❌ Endpoint deployment failed with status: {current_status}")
                    print("💡 Check Databricks UI for detailed error information.")
                    exit()
                    
            time.sleep(check_interval)
            
        except Exception as status_error:
            print(f"⚠️ Status check error: {status_error}")
            time.sleep(check_interval)
    
    # Timeout handling
    elapsed_time = time.time() - start_time
    print(f"⏰ Timeout after {elapsed_time:.1f} seconds.")
    print("ℹ️ Endpoint deployment is still in progress.")
    print("💡 Options:")
    print("   1. Check Databricks UI: Machine Learning > Model Serving")
    print("   2. Run this command to check status later:")
    print(f"      deploy_client.get_endpoint('{endpoint_name}')")
    print("   3. The endpoint may still complete deployment in the background")

except Exception as e:
    print(f"❌ Endpoint creation/update failed: {e}")
    print("\n🔍 Troubleshooting:")
    print("1. Check model serving permissions in Databricks")
    print("2. Verify model is properly registered")
    print("3. Try creating endpoint manually in Databricks UI")
    print("4. Check if workspace has sufficient compute resources")

print("🏁 Script execution completed.")