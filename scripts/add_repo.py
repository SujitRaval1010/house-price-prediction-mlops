from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound # NotFound त्रुटि को संभालने के लिए

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

# -----------------------------------------------------------------------------------
# सुधार: अब स्क्रिप्ट अधिक मजबूती से रेपो की स्थिति को संभालती है।
# -----------------------------------------------------------------------------------
repo = None
try:
    # एक अधिक मजबूत विधि का उपयोग करके रेपो के अस्तित्व की जांच करें
    repos_list = list(w.repos.list(path_prefix=repo_path))
    if repos_list:
        repo = repos_list[0]
    else:
        raise NotFound(f"No repo found at path: {repo_path}")
except NotFound:
    print(f"Repository not found at {repo_path}. Creating it...")
    repo = w.repos.create(
        url=repo_url,
        provider=repo_provider,
        path=repo_path
    )
    print(f"Repository added successfully! Repo ID: {repo.id}")
    print("ℹ️ To sync with remote changes in the future, just run this script again.")

if repo:
    # Update the repository by fetching the latest changes from the remote.
    w.repos.update(
        repo_id=repo.id,
        branch="main" 
    )
    print("✅ Repository updated successfully!")
