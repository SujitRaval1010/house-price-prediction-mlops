import os
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from databricks.sdk.service.jobs import TaskDependency

# =====================================================================
# 1️⃣ Initialize Databricks Client
# =====================================================================
try:
    # Ensure env vars are loaded
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")

    if not host or not token:
        raise EnvironmentError("DATABRICKS_HOST or DATABRICKS_TOKEN not set.")

    # Initialize with explicit host/token (important for CI/CD)
    w = WorkspaceClient(host=host, token=token)
    print("✅ Databricks client initialized successfully")
    print(f"🔗 Workspace: {host}")
except Exception as e:
    print(f"❌ Databricks client initialization failed: {e}")
    exit(1)

# =====================================================================
# 2️⃣ Configuration
# =====================================================================
repo_name = "house-price-prediction-mlops"
repo_path = f"/Repos/vipultak7171@gmail.com/{repo_name}"

print("\n" + "=" * 60)
print("🚀 MLOPS PIPELINE ORCHESTRATION (Serverless)")
print("=" * 60)
print(f"📁 Repository Path: {repo_path}")
print("-" * 60 + "\n")

# =====================================================================
# 3️⃣ Helper Functions
# =====================================================================

def get_job_id(job_name):
    """Get existing Databricks job ID by name."""
    try:
        for j in w.jobs.list():
            if j.settings.name == job_name:
                return j.job_id
        return None
    except Exception as e:
        print(f"⚠️ Error while listing jobs: {e}")
        return None


def create_or_update_job(job_name, tasks, auto_run=False):
    """Create or update Databricks job with the given tasks."""
    try:
        existing_job_id = get_job_id(job_name)
        job_settings = jobs.JobSettings(
            name=job_name,
            tasks=tasks,
            max_concurrent_runs=1
        )

        if existing_job_id:
            print(f"🔄 Updating job: {job_name} (ID: {existing_job_id})")
            w.jobs.reset(job_id=existing_job_id, new_settings=job_settings)
            job_id = existing_job_id
        else:
            print(f"➕ Creating new job: {job_name}")
            result = w.jobs.create(
                name=job_settings.name,
                tasks=job_settings.tasks,
                max_concurrent_runs=1
            )
            job_id = result.job_id

        print(f"✅ Job ready: {job_name} (ID: {job_id})")

        if auto_run:
            print(f"🚀 Triggering job run: {job_name}")
            run_result = w.jobs.run_now(job_id=job_id)
            print(f"   📋 Run ID: {run_result.run_id}")
            return job_id, run_result.run_id

        return job_id, None

    except Exception as e:
        print(f"❌ Error in job '{job_name}': {e}")
        return None, None


def wait_for_job_completion(job_id, run_id, job_name, timeout_minutes=30):
    """Wait for Databricks job completion."""
    if not run_id:
        return False

    print(f"\n⏳ Waiting for {job_name} to complete (max {timeout_minutes} min)...")
    start_time = time.time()
    last_state = None

    while time.time() - start_time < timeout_minutes * 60:
        try:
            run_info = w.jobs.get_run(run_id=run_id)
            state = run_info.state.life_cycle_state.value if run_info.state else "UNKNOWN"

            if state != last_state:
                print(f"   📊 State: {state}")
                last_state = state

            if state == "TERMINATED":
                result_state = run_info.state.result_state.value if run_info.state else "UNKNOWN"
                if result_state == "SUCCESS":
                    print(f"✅ {job_name} completed successfully.")
                    return True
                else:
                    print(f"❌ {job_name} failed. Status: {result_state}")
                    return False

            time.sleep(15)
        except Exception as e:
            print(f"⚠️ Error checking job status: {e}")
            time.sleep(30)

    print(f"⏰ Timeout waiting for {job_name}.")
    return False


# =====================================================================
# 4️⃣ DEV JOB (Data Ingest -> Train -> Register)
# =====================================================================
print("\n[STEP 1/3] 🛠️ Creating DEV Training Pipeline...")
print("-" * 60)

dev_tasks = [
    jobs.Task(
        task_key="data_ingest_and_prep_task",
        notebook_task=jobs.NotebookTask(
            notebook_path=f"{repo_path}/dev_env/Data_Ingestion_and_Preparation",
            base_parameters={"environment": "development"}
        )
    ),
    jobs.Task(
        task_key="model_training_task",
        notebook_task=jobs.NotebookTask(
            notebook_path=f"{repo_path}/dev_env/Model_Training",
            base_parameters={"environment": "development"}
        ),
        depends_on=[TaskDependency(task_key="data_ingest_and_prep_task")]
    ),
    jobs.Task(
        task_key="model_registration_task",
        notebook_task=jobs.NotebookTask(
            notebook_path=f"{repo_path}/dev_env/Model_Registration",
            base_parameters={"environment": "development"}
        ),
        depends_on=[TaskDependency(task_key="model_training_task")]
    )
]

dev_job_id, dev_run_id = create_or_update_job(
    "1. dev-ml-training-pipeline",
    tasks=dev_tasks,
    auto_run=True
)

if not dev_run_id or not wait_for_job_completion(dev_job_id, dev_run_id, "DEV Training Pipeline", 25):
    print("❌ Stopping pipeline as DEV failed.")
    exit(1)

# =====================================================================
# 5️⃣ UAT JOB (Model Staging -> Inference Test)
# =====================================================================
print("\n[STEP 2/3] 🧪 Creating UAT Pipeline...")
print("-" * 60)

uat_tasks = [
    jobs.Task(
        task_key="model_staging_task",
        notebook_task=jobs.NotebookTask(
            notebook_path=f"{repo_path}/uat_env/model_staging_uat",
            base_parameters={"alias": "Staging"}
        )
    ),
    jobs.Task(
        task_key="inference_test_task",
        notebook_task=jobs.NotebookTask(
            notebook_path=f"{repo_path}/uat_env/Model-Inference",
            base_parameters={"alias": "Staging", "environment": "uat"}
        ),
        depends_on=[TaskDependency(task_key="model_staging_task")]
    )
]

uat_job_id, uat_run_id = create_or_update_job(
    "2. uat-ml-inference-pipeline",
    tasks=uat_tasks,
    auto_run=True
)

if not uat_run_id or not wait_for_job_completion(uat_job_id, uat_run_id, "UAT Pipeline", 20):
    print("❌ Stopping pipeline as UAT failed.")
    exit(1)

# =====================================================================
# 6️⃣ PROD JOB (Promotion -> Serving -> Inference)
# =====================================================================
print("\n[STEP 3/3] 🚀 Creating PROD Deployment Pipeline...")
print("-" * 60)

prod_tasks = [
    jobs.Task(
        task_key="model_promotion_task",
        notebook_task=jobs.NotebookTask(
            notebook_path=f"{repo_path}/prod_env/model-promotion",
            base_parameters={"alias": "Production", "action": "promote"}
        )
    ),
    jobs.Task(
        task_key="serving_endpoint_task",
        notebook_task=jobs.NotebookTask(
            notebook_path=f"{repo_path}/prod_env/create-serving-endpoint",
            base_parameters={"environment": "prod"}
        ),
        depends_on=[TaskDependency(task_key="model_promotion_task")]
    ),
    jobs.Task(
        task_key="model_inference_production",
        notebook_task=jobs.NotebookTask(
            notebook_path=f"{repo_path}/prod_env/Model-Inference",
            base_parameters={"alias": "Production", "environment": "prod"}
        ),
        depends_on=[TaskDependency(task_key="serving_endpoint_task")]
    )
]

prod_job_id, prod_run_id = create_or_update_job(
    "3. prod-ml-deployment-pipeline",
    tasks=prod_tasks,
    auto_run=True
)

wait_for_job_completion(prod_job_id, prod_run_id, "PROD Deployment Pipeline", 30)

print("\n" + "=" * 60)
print("🎉 MLOPS PIPELINE COMPLETED SUCCESSFULLY!")
print("=" * 60)
