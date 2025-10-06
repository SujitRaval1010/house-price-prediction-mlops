import os
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from databricks.sdk.service.jobs import TaskDependency

# Initialize Databricks client
try:
    w = WorkspaceClient()
    print("✅ Databricks client initialized successfully")
except Exception as e:
    print(f"❌ त्रुटि: Databricks क्लाइंट को आरंभ करने में विफल।")
    print(f"   सुनिश्चित करें कि DATABRICKS_HOST और DATABRICKS_TOKEN सेट हैं।")
    print(f"   विवरण: {e}")
    exit(1)

# Configuration
repo_name = "house-price-prediction-mlops"
repo_path = f"/Repos/vipultak7171@gmail.com/{repo_name}"

print("\n" + "=" * 60)
print("🚀 MLOPS PIPELINE ORCHESTRATION (Serverless)")
print("=" * 60)
print(f"📁 Repository Path: {repo_path}")
print("-" * 60 + "\n")

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def get_job_id(job_name):
    """नाम से मौजूदा Databricks Job ID प्राप्त करें।"""
    try:
        job = next(j for j in w.jobs.list() if j.settings.name == job_name)
        return job.job_id
    except StopIteration:
        return None
    except Exception as e:
        print(f"⚠️ चेतावनी: Job list करते समय त्रुटि: {e}")
        return None


def create_or_update_job(job_name, tasks, auto_run=False):
    """
    Job बनाता है, अपडेट करता है, और वैकल्पिक रूप से उसे ट्रिगर करता है।
    Serverless compute के लिए कोई cluster configuration की आवश्यकता नहीं।
    """
    try:
        existing_job_id = get_job_id(job_name)
        
        # Serverless के लिए job_clusters parameter नहीं चाहिए
        job_settings = jobs.JobSettings(
            name=job_name,
            tasks=tasks,
            # Serverless automatically compute allocate करता है
            max_concurrent_runs=1  # एक समय में एक ही run
        )
        
        if existing_job_id:
            print(f"🔄 Job को अपडेट कर रहा है: {job_name} (ID: {existing_job_id})")
            w.jobs.reset(job_id=existing_job_id, new_settings=job_settings)
            job_id = existing_job_id
        else:
            print(f"➕ नया Job बना रहा है: {job_name}")
            result = w.jobs.create(
                name=job_settings.name,
                tasks=job_settings.tasks,
                max_concurrent_runs=1
            )
            job_id = result.job_id
            
        print(f"✅ Job '{job_name}' तैयार है (ID: {job_id})")
        
        if auto_run:
            print(f"🚀 Job को ऑटो-ट्रिगर कर रहा है: {job_name}")
            run_result = w.jobs.run_now(job_id=job_id)
            print(f"   📋 Run ID: {run_result.run_id}")
            return job_id, run_result.run_id
            
        return job_id, None
        
    except Exception as e:
        print(f"❌ Job '{job_name}' में त्रुटि: {e}")
        print(f"   विस्तृत त्रुटि: {str(e)}")
        return None, None


def wait_for_job_completion(job_id, run_id, job_name, timeout_minutes=30):
    """Job के पूरा होने की प्रतीक्षा करें और स्थिति की जाँच करें।"""
    if not run_id:
        return False
        
    print(f"\n⏳ {job_name} के पूरा होने की प्रतीक्षा कर रहा है...")
    print(f"   (अधिकतम समय: {timeout_minutes} मिनट)")
    
    timeout_seconds = timeout_minutes * 60
    start_time = time.time()
    last_state = None
    
    while time.time() - start_time < timeout_seconds:
        try:
            run_info = w.jobs.get_run(run_id=run_id)
            state = run_info.state.life_cycle_state.value if run_info.state and run_info.state.life_cycle_state else "UNKNOWN"
            
            # State change होने पर ही print करें
            if state != last_state:
                print(f"   📊 {job_name} स्थिति: {state}")
                last_state = state
            
            if state == "TERMINATED":
                result_state = run_info.state.result_state.value if run_info.state and run_info.state.result_state else "UNKNOWN"
                if result_state == "SUCCESS":
                    elapsed_time = int(time.time() - start_time)
                    print(f"✅ {job_name} सफलतापूर्वक पूरा हुआ! (समय: {elapsed_time}s)")
                    return True
                else:
                    print(f"❌ {job_name} विफल हो गया। स्थिति: {result_state}")
                    if run_info.state.state_message:
                        print(f"   त्रुटि संदेश: {run_info.state.state_message}")
                    return False
            
            time.sleep(15)  # हर 15 सेकंड में जाँच करें
                
        except Exception as e:
            print(f"   ⚠️ स्थिति जाँचने में त्रुटि: {e}")
            time.sleep(30)
    
    print(f"⏰ {job_name} के लिए टाइमआउट हो गया ({timeout_minutes} मिनट)।")
    return False


# ==============================================================================
# 1. DEV JOB CREATION (Data Ingest -> Train -> Register)
# ==============================================================================
print("\n[STEP 1/3] 🛠️ DEV Training Pipeline बना रहा है...")
print("-" * 60)

dev_tasks = [
    # Task 1: Data Ingestion and Preparation
    jobs.Task(
        task_key="data_ingest_and_prep_task",
        description="Ingests and prepares data for training",
        notebook_task=jobs.NotebookTask(
            notebook_path=f"{repo_path}/dev_env/Data_Ingestion_and_Preparation",
            base_parameters={"environment": "development"}
        )
        # Serverless compute automatically assign होगा, कोई cluster key नहीं
    ),
    
    # Task 2: Model Training
    jobs.Task(
        task_key="model_training_task",
        description="Trains the model using prepared data",
        notebook_task=jobs.NotebookTask(
            notebook_path=f"{repo_path}/dev_env/Model_Training",
            base_parameters={"environment": "development"}
        ),
        depends_on=[TaskDependency(task_key="data_ingest_and_prep_task")]
    ),
    
    # Task 3: Model Registration
    jobs.Task(
        task_key="model_registration_task",
        description="Registers the trained model to Unity Catalog",
        notebook_task=jobs.NotebookTask(
            notebook_path=f"{repo_path}/dev_env/Model_Registration",
            base_parameters={"environment": "development"}
        ),
        depends_on=[TaskDependency(task_key="model_training_task")]
    )
]

dev_job_id, dev_run_id = create_or_update_job(
    job_name="1. dev-ml-training-pipeline", 
    tasks=dev_tasks,
    auto_run=True 
)

# ==============================================================================
# WAIT FOR DEV COMPLETION
# ==============================================================================
if dev_job_id and dev_run_id:
    dev_success = wait_for_job_completion(
        dev_job_id, 
        dev_run_id, 
        "DEV Training Pipeline", 
        timeout_minutes=25
    )
else:
    print("\n❌ DEV Job को ट्रिगर नहीं कर सका। Pipeline रोक रहा है।")
    exit(1)

# ==============================================================================
# 2. UAT JOB CREATION (Staging -> Inference Test)
# ==============================================================================
if dev_success:
    print("\n[STEP 2/3] 🧪 UAT Testing Pipeline बना रहा है...")
    print("-" * 60)
    
    uat_tasks = [
        # Task 1: Model Staging
        jobs.Task(
            task_key="model_staging_task",
            description="Promotes the latest model version to Staging alias",
            notebook_task=jobs.NotebookTask(
                notebook_path=f"{repo_path}/uat_env/model_staging_uat", 
                base_parameters={"alias": "Staging"}
            )
        ),
        
        # Task 2: Inference Test
        jobs.Task(
            task_key="inference_test_task",
            description="Runs inference tests against the Staging model",
            notebook_task=jobs.NotebookTask(
                notebook_path=f"{repo_path}/uat_env/Model-Inference",
                base_parameters={"alias": "Staging", "environment": "uat"}
            ),
            depends_on=[TaskDependency(task_key="model_staging_task")]
        )
    ]
    
    uat_job_id, uat_run_id = create_or_update_job(
        job_name="2. uat-ml-inference-pipeline",
        tasks=uat_tasks,
        auto_run=True 
    )
    
    # Wait for UAT completion
    if uat_job_id and uat_run_id:
        uat_success = wait_for_job_completion(
            uat_job_id, 
            uat_run_id, 
            "UAT Testing Pipeline", 
            timeout_minutes=20
        )
    else:
        print("\n❌ UAT Job को ट्रिगर नहीं कर सका। Pipeline रोक रहा है।")
        uat_success = False
    
    # ==========================================================================
    # 3. PROD JOB CREATION (Promotion -> Serving Endpoint)
    # ==========================================================================
    if uat_success:
        print("\n[STEP 3/3] 🚀 PROD Deployment Pipeline बना रहा है...")
        print("-" * 60)
        
        prod_tasks = [
            # Task 1: Model Promotion
            jobs.Task(
                task_key="model_promotion_task",
                description="Promotes the model from Staging to Production alias",
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{repo_path}/prod_env/model-promotion", 
                    base_parameters={"alias": "Production", "action": "promote"}
                )
            ),
            
            # Task 2: Serving Endpoint Deployment
            jobs.Task(
                task_key="serving_endpoint_task",
                description="Creates/updates Serving Endpoint with Production model",
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{repo_path}/prod_env/create-serving-endpoint", 
                    base_parameters={"environment": "prod"}
                ),
                depends_on=[TaskDependency(task_key="model_promotion_task")]
            ),
            # Task 3: Model Inference in Production
            jobs.Task(
                task_key="model_inference_production",
                description="Runs inference tests against the Production model",
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{repo_path}/prod_env/Model-Inference",
                    base_parameters={"alias": "Production", "environment": "prod"}
                ),
                depends_on=[TaskDependency(task_key="serving_endpoint_task")]
            )
        ]
         
        prod_job_id, prod_run_id = create_or_update_job(
            job_name="3. prod-ml-deployment-pipeline",
            tasks=prod_tasks,
            auto_run=True 
        )
        
        # Wait for PROD completion
        if prod_job_id and prod_run_id:
            prod_success = wait_for_job_completion(
                prod_job_id, 
                prod_run_id, 
                "PROD Deployment Pipeline", 
                timeout_minutes=30
            )
            
            if prod_success:
                print("\n" + "=" * 60)
                print("🎉 MLOPS PIPELINE EXECUTION SUCCESSFUL! 🎉")
                print("=" * 60)
                print("✅ DEV: Model trained and registered")
                print("✅ UAT: Model validated in staging")
                print("✅ PROD: Model deployed to production")
                print("=" * 60)
                print("\n💡 Next Steps:")
                print("   1. Check your Unity Catalog for registered models")
                print("   2. Verify the Serving Endpoint in Databricks UI")
                print("   3. Test the production endpoint with sample data")
                print("=" * 60)
            else:
                print("\n❌ PROD Pipeline विफल रहा। Deployment logs जाँचें।")
        else:
            print("\n❌ PROD Job को ट्रिगर नहीं कर सका।")
    else:
        print("\n⏭️ UAT Pipeline विफल रहा, PROD deployment छोड़ रहा है।")
else:
    print("\n⏭️ DEV Pipeline विफल रहा, आगे की pipelines छोड़ रहा है।")

print("\n" + "=" * 60)
print("🏁 MLOps Pipeline Orchestration Script Complete!")
print("=" * 60)