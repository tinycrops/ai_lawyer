import json
from huggingface_hub import hf_hub_download

# Download the metadata file
file_path = hf_hub_download(
    repo_id="the-ride-never-ends/american_law", 
    filename="american_law/metadata/485575.json", 
    repo_type="dataset"
)

# Read and display the metadata
with open(file_path, "r") as f:
    data = json.load(f)
    
print(json.dumps(data, indent=2))
