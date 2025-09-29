import os
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from databricks.sdk.service.jobs import TaskDependency

# Initialize Databricks client
w = WorkspaceClient()

# Configuration
repo_name = "house-price-prediction-mlops"
repo_path = f"/Repos/vipultak7171@gmail.com/{repo_name}"

print("Creating jobs and auto-triggering pipeline...")

# --- 1. CLUSTER CONFIGURATION (MANDATORY for multi-task jobs) ---
# Defining a single cluster configuration to be used by all job tasks.
cluster_config = jobs.JobCluster(
    job_cluster_key="base_cluster",
    new_cluster=jobs.NewCluster(
        spark_version="13.3.x-cpu-ml-scala2.12", # Use a standard ML Runtime
        node_type_id="Standard_DS3_v2",
        num_workers=0, # Single node cluster for job execution
        spark_env_vars={"PYSPARK_PYTHON": "/databricks/python/bin/python"},
    )
)
# ------------------------------------------------------------------

def get_job_id(job_name):
    """Get job ID by name"""
    try:
        # Note: Using list() and iterating is safer than relying on job.get_by_name()
        job = next(j for j in w.jobs.list() if j.settings.name == job_name)
        return job.job_id
    except StopIteration:
        return None

# UPDATED: Added job_clusters parameter
def create_or_update_job(job_name, tasks, job_clusters=None, auto_run=False):
    """Create or update job and optionally auto-run it"""
    try:
        existing_job_id = get_job_id(job_name)
        
        # JobSettings updated to include job_clusters
        job_settings = jobs.JobSettings(
            name=job_name,
            tasks=tasks,
            job_clusters=job_clusters # Pass cluster config
        )
        
        if existing_job_id:
            print(f"Updating job: {job_name}")
            # FIX 1: reset() requires job_id and new_settings (JobSettings object)
            w.jobs.reset(job_id=existing_job_id, new_settings=job_settings)
            job_id = existing_job_id
        else:
            print(f"Creating job: {job_name}")
            # FIX 2: create() now includes job_clusters
            result = w.jobs.create(
                name=job_settings.name,
                tasks=job_settings.tasks,
                job_clusters=job_clusters # Pass cluster config during creation
            )
            
            job_id = result.job_id
            
        print(f"Job '{job_name}' ready (ID: {job_id})")
        
        # Auto-trigger if requested
        if auto_run:
            print(f"Auto-triggering job: {job_name}")
            run_result = w.jobs.run_now(job_id=job_id)
            print(f"Job triggered! Run ID: {run_result.run_id}")
            return job_id, run_result.run_id
            
        return job_id, None
        
    except Exception as e:
        print(f"Error with job {job_name}: {e}")
        return None, None

def wait_for_job_completion(job_id, run_id, job_name, timeout_minutes=30):
    """Wait for job completion"""
    if not run_id:
        return False
        
    print(f"Waiting for {job_name} to complete (max {timeout_minutes} minutes)...")
    
    timeout_seconds = timeout_minutes * 60
    start_time = time.time()
    
    while time.time() - start_time < timeout_seconds:
        try:
            run_info = w.jobs.get_run(run_id=run_id)
            state = run_info.state.life_cycle_state.value if run_info.state and run_info.state.life_cycle_state else "UNKNOWN"
            
            if state == "TERMINATED":
                result_state = run_info.state.result_state.value if run_info.state and run_info.state.result_state else "UNKNOWN"
                if result_state == "SUCCESS":
                    print(f"✅ {job_name} completed successfully!")
                    return True
                else:
                    print(f"❌ {job_name} failed with state: {result_state}")
                    return False
            elif state in ["PENDING", "RUNNING"]:
                print(f"⏳ {job_name} status: {state}")
                time.sleep(30)  # Check every 30 seconds
            else:
                print(f"🔍 {job_name} status: {state}")
                time.sleep(30)
                
        except Exception as e:
            print(f"Error checking {job_name} status: {e}")
            time.sleep(30)
    
    print(f"⏰ Timeout waiting for {job_name}")
    return False

# 1. Create DEV Job (UPDATED with 3 sequential Notebook tasks)
print("\n1. Creating DEV Training Job...")
dev_tasks = [
    # Task 1: Data Ingestion and Preparation
    jobs.Task(
        task_key="data_ingest_and_prep_task",
        description="Ingests and prepares data for training.",
        notebook_task=jobs.NotebookTask(
            notebook_path=f"{repo_path}/dev_env/Data_Ingestion_and_Preparation",
            base_parameters={"environment": "development"}
        ),
        job_cluster_key="base_cluster" # Link to defined cluster
    ),
    # Task 2: Model Training
    jobs.Task(
        task_key="model_training_task",
        description="Trains the model using prepared data.",
        notebook_task=jobs.NotebookTask(
            notebook_path=f"{repo_path}/dev_env/Model_Training",
            base_parameters={"environment": "development"}
        ),
        depends_on=[TaskDependency(task_key="data_prep_task")], # Dependency added
        job_cluster_key="base_cluster" # Link to defined cluster
    ),
    # Task 3: Model Registration
    jobs.Task(
        task_key="model_registration_task",
        description="Registers the trained model to MLflow.",
        notebook_task=jobs.NotebookTask(
            notebook_path=f"{repo_path}/dev_env/Model_Registration",
            base_parameters={"environment": "development"}
        ),
        depends_on=[TaskDependency(task_key="model_training_task")], # Dependency added
        job_cluster_key="base_cluster" # Link to defined cluster
    )
]

dev_job_id, dev_run_id = create_or_update_job(
    job_name="dev-ml-training-pipeline", 
    tasks=dev_tasks, 
    job_clusters=[cluster_config], # Cluster config pass kiya
    auto_run=True  # Automatically start DEV job
)

# 2. Wait for DEV completion, then trigger UAT
if dev_job_id and dev_run_id:
    dev_success = wait_for_job_completion(dev_job_id, dev_run_id, "DEV Training", timeout_minutes=20)
    
    if dev_success:
        print("\n2. Creating and triggering UAT Job...")
        
        uat_tasks = [
            jobs.Task(
                task_key="staging_task",
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{repo_path}/uat_env/model_staging_uat",
                    base_parameters={"alias": "Staging"}
                ),
                job_cluster_key="base_cluster" # Link to defined cluster
            ),
            jobs.Task(
                task_key="inference_task",
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{repo_path}/uat_env/Model-Inference",
                    base_parameters={"alias": "Staging", "environment": "uat"}
                ),
                depends_on=[TaskDependency(task_key="staging_task")],
                job_cluster_key="base_cluster" # Link to defined cluster
            )
        ]
        
        uat_job_id, uat_run_id = create_or_update_job(
            job_name="uat-ml-inference-pipeline",
            tasks=uat_tasks,
            job_clusters=[cluster_config], # Cluster config pass kiya
            auto_run=True  # Auto-trigger UAT after DEV success
        )
        
        # 3. Wait for UAT completion, then trigger PROD
        if uat_job_id and uat_run_id:
            uat_success = wait_for_job_completion(uat_job_id, uat_run_id, "UAT Staging", timeout_minutes=15)
            
            if uat_success:
                print("\n3. Creating and triggering PROD Job with Serving Endpoint...")
                
                prod_tasks = [
                    # Model Promotion Task 
                    jobs.Task(
                        task_key="promotion_task",
                        notebook_task=jobs.NotebookTask(
                            # This path is where the user confirmed the promotion logic is located
                            notebook_path=f"{repo_path}/prod_env/Model-Inference", 
                            base_parameters={"alias": "Production"}
                        ),
                        job_cluster_key="base_cluster" # Link to defined cluster
                    ),
                    
                    # Serving Endpoint Task
                    jobs.Task(
                        task_key="serving_endpoint_task",
                        notebook_task=jobs.NotebookTask(
                            # Assuming this is a notebook path based on context
                            notebook_path=f"{repo_path}/prod_env/create-serving-endpoint", 
                            base_parameters={
                                "model_name": "workspace.ml.house_price_model",
                                "alias": "Production",
                                "endpoint_name": "house-price-endpoint"
                            }
                        ),
                        # Serving endpoint must wait for the model to be promoted to "Production"
                        depends_on=[TaskDependency(task_key="promotion_task")],
                        job_cluster_key="base_cluster" # Link to defined cluster
                    )
                ]
                
                prod_job_id, prod_run_id = create_or_update_job(
                    job_name="prod-ml-deployment-pipeline",
                    tasks=prod_tasks,
                    job_clusters=[cluster_config], # Cluster config pass kiya
                    auto_run=True  # Auto-trigger PROD after UAT success
                )
                
                # 4. Wait for PROD completion
                if prod_job_id and prod_run_id:
                    prod_success = wait_for_job_completion(prod_job_id, prod_run_id, "PROD Deployment", timeout_minutes=25)
                    
                    if prod_success:
                        print("\n🎉 COMPLETE CI/CD PIPELINE EXECUTED SUCCESSFULLY!")
                        print("=" * 60)
                        print("✅ DEV: Data Prep, Training, Registration - COMPLETED")
                        print("✅ UAT: Model Staging & Inference - COMPLETED") 
                        print("✅ PROD: Model Promotion & Serving - COMPLETED")
                        print("🚀 Your model is now live on serving endpoint!")
                        print("=" * 60)
                    else:
                        print("❌ PROD job failed")
                else:
                    print("❌ Could not trigger PROD job")
            else:
                print("❌ UAT job failed, skipping PROD")
        else:
            print("❌ Could not trigger UAT job")
    else:
        print("❌ DEV job failed, stopping pipeline")
else:
    print("❌ Could not create/trigger DEV job")

# Final summary
print("\n📊 Pipeline Execution Summary:")
try:
    all_jobs = list(w.jobs.list())
    mlops_jobs = [j for j in all_jobs if 'ml' in j.settings.name.lower()]
    
    for job in mlops_jobs:
        print(f"📋 {job.settings.name} (ID: {job.job_id})")
        
        # Get latest run status
        try:
            runs = list(w.jobs.list_runs(job.job_id, limit=1))
            if runs:
                latest_run = runs[0]
                status = latest_run.state.result_state.value if latest_run.state and latest_run.state.result_state else "RUNNING"
                print(f"   └── Latest run status: {status}")
        except Exception: 
            print("   └── No recent runs")
            
except Exception as e:
    print(f"Error getting job summary: {e}")

print("\n🏁 Auto-triggered MLOps Pipeline Complete!")
print("All jobs created and executed automatically in sequence.")
