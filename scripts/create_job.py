import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

# Initialize Databricks client from environment variables
w = WorkspaceClient()

# Define some common variables
repo_name = "house-price-prediction-mlops"
repo_path = f"/Repos/vipultak7171@gmail.com/{repo_name}"

print("Creating jobs for Databricks Community Edition...")
print("Note: Using existing compute resources (no new cluster creation)")

def create_community_job(job_name, python_file_path, parameters=None):
    """Community Edition के लिए job create करने का helper function"""
    
    try:
        # Check if job already exists
        existing_job = None
        try:
            existing_job = next(j for j in w.jobs.list() if j.settings.name == job_name)
            print(f"Job '{job_name}' already exists. Updating it...")
            
            task_config = jobs.Task(
                task_key=f"{job_name.replace('-', '_')}_task",
                notebook_task=jobs.NotebookTask(
                    notebook_path=python_file_path,
                    base_parameters=parameters or {}
                )
            )
            
            w.jobs.reset(
                job_id=existing_job.job_id,
                new_settings=jobs.JobSettings(
                    name=job_name,
                    tasks=[task_config],
                )
            )
            print(f"Job '{job_name}' updated successfully!")
            
        except StopIteration:
            print(f"Job '{job_name}' not found. Creating new job...")
            
            task_config = jobs.Task(
                task_key=f"{job_name.replace('-', '_')}_task",
                notebook_task=jobs.NotebookTask(
                    notebook_path=python_file_path,
                    base_parameters=parameters or {}
                )
            )
            
            w.jobs.create(
                name=job_name,
                tasks=[task_config],
            )
            print(f"Job '{job_name}' created successfully!")
            
    except Exception as e:
        print(f"Error with job '{job_name}': {e}")
        print("Trying alternative approach...")
        
        # Alternative: Create as notebook job instead of python file
        try:
            notebook_path = python_file_path.replace('.py', '')  # Remove .py extension for notebook
            
            task_config = jobs.Task(
                task_key=f"{job_name.replace('-', '_')}_task",
                notebook_task=jobs.NotebookTask(
                    notebook_path=notebook_path,
                    base_parameters=parameters or {}
                )
            )
            
            if existing_job:
                w.jobs.reset(
                    job_id=existing_job.job_id,
                    new_settings=jobs.JobSettings(
                        name=job_name,
                        tasks=[task_config]
                    )
                )
            else:
                w.jobs.create(
                    name=job_name,
                    tasks=[task_config]
                )
            
            print(f"Job '{job_name}' created with notebook task!")
            
        except Exception as notebook_error:
            print(f"Notebook approach also failed: {notebook_error}")

# --- Development Job ---
print("\n1. Creating Development Job...")
create_community_job(
    job_name="dev-ml-training-pipeline",
    python_file_path=f"{repo_path}/dev_env/Model-Training",
    parameters={"environment": "development"}
)

# --- UAT Job with two tasks ---
print("\n2. Creating UAT Job with multiple tasks...")
uat_job_name = "uat-ml-inference-pipeline"
uat_tasks = [
    # Task 1: Staging the model
    jobs.Task(
        task_key="staging_task",
        notebook_task=jobs.NotebookTask(
            notebook_path=f"{repo_path}/uat_env/model_staging_uat",
            base_parameters={"alias": "Staging"}
        )
    ),
    # Task 2: Running inference on the staged model
    jobs.Task(
        task_key="inference_task",
        notebook_task=jobs.NotebookTask(
            notebook_path=f"{repo_path}/uat_env/Model-Inference",
            # FIX: environment parameter add करें
            base_parameters={"alias": "Staging", "environment": "uat"}
        ),
        depends_on=[jobs.TaskDependency(task_key="staging_task")]
    )
]

try:
    # Check if job already exists
    existing_uat_job = next((j for j in w.jobs.list() if j.settings.name == uat_job_name), None)
    
    if existing_uat_job:
        print(f"Job '{uat_job_name}' already exists. Updating it...")
        w.jobs.reset(
            job_id=existing_uat_job.job_id,
            new_settings=jobs.JobSettings(name=uat_job_name, tasks=uat_tasks)
        )
        print(f"Job '{uat_job_name}' updated with multi-task pipeline!")
    else:
        print(f"Job '{uat_job_name}' not found. Creating new multi-task job...")
        w.jobs.create(name=uat_job_name, tasks=uat_tasks)
        print(f"Job '{uat_job_name}' created successfully!")
except Exception as e:
    print(f"❌ Error with UAT job '{uat_job_name}': {e}")
    

# --- Production Job ---
print("\n3. Creating Production Job...")
create_community_job(
    job_name="prod-ml-inference-pipeline",
    python_file_path=f"{repo_path}/prod_env/Model-Inference", 
    # FIX: Unity Catalog के लिए 'production' के बजाय 'Production' alias का उपयोग करें
    parameters={"alias": "Production", "environment": "production"}
)

print("\n" + "="*50)
print("COMMUNITY EDITION JOB CREATION SUMMARY")
print("="*50)
print("✅ Jobs configured for serverless compute")
print("✅ No custom cluster configuration needed")
print("ℹ️ Jobs will use shared Databricks compute resources")
print("⚠️ Performance may be limited compared to dedicated clusters")
print("="*50)

# List all created jobs for verification
print("\nListing all jobs to verify creation:")
try:
    all_jobs = list(w.jobs.list())
    mlops_jobs = [j for j in all_jobs if 'ml' in j.settings.name.lower()]
    
    if mlops_jobs:
        print("Found MLOps jobs:")
        for job in mlops_jobs:
            print(f"  - {job.settings.name} (ID: {job.job_id})")
    else:
        print("No MLOps jobs found yet")
        
except Exception as e:
    print(f"Error listing jobs: {e}")
