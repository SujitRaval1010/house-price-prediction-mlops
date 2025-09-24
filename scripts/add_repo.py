from databricks.sdk import WorkspaceClient

# Initialize the Databricks Workspace Client.
w = WorkspaceClient()

# Define the repository details.
repo_url = "https://github.com/SujitRaval1010/house-price-prediction-mlops.git"
repo_provider = "gitHub"

# -----------------------------------------------------------------------------------
# FIX: रेपो पाथ को अपने यूज़रनेम फ़ोल्डर के साथ अपडेट करें
# यह सुनिश्चित करेगा कि रेपो सही स्थान पर बनाया गया है।
# -----------------------------------------------------------------------------------
repo_path = "/Repos/vipultak7171@gmail.com/house-price-prediction-mlops"

# Create or update the repository.
try:
    # Check if the repository already exists.
    existing_repos = w.repos.get_by_path(repo_path)
    print(f"Repository already exists at {repo_path}. Updating it...")
    # Update the repository to ensure it's on the correct branch if needed.
    w.repos.update(
        repo_id=existing_repos.id,
        branch="main" # Or any other branch you want to start with.
    )
except Exception:
    # If the repository doesn't exist, create it.
    print(f"Repository not found at {repo_path}. Creating it...")
    new_repo = w.repos.create(
        url=repo_url,
        provider=repo_provider,
        path=repo_path
    )
    print(f"Repository added successfully! Repo ID: {new_repo.id}")
