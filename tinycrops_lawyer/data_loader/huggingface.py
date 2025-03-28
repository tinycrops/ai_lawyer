"""
HuggingFace dataset loader for the American Law Dataset.
Handles downloading and caching of dataset files.
"""

import os
import json
import logging
from pathlib import Path
from typing import Optional, Dict, List, Any, Union

import pandas as pd
from huggingface_hub import hf_hub_download, list_repo_files

from tinycrops_lawyer.config import DATA_DIR

# Initialize logger
logger = logging.getLogger(__name__)

# Constants
REPO_ID = 'the-ride-never-ends/american_law'
REPO_TYPE = 'dataset'

def list_dataset_files(subfolder: Optional[str] = None) -> List[str]:
    """
    List files available in the HuggingFace dataset repo.
    
    Args:
        subfolder: Optional subfolder path to list files from
        
    Returns:
        List[str]: List of file paths in the repository
    """
    try:
        path_filter = f"{subfolder}/" if subfolder else None
        files = list_repo_files(
            repo_id=REPO_ID,
            repo_type=REPO_TYPE,
            path_filter=path_filter
        )
        return files
    except Exception as e:
        logger.error(f"Error listing dataset files: {e}")
        return []

def download_and_cache_file(file_path: str) -> Optional[str]:
    """
    Download a file from HuggingFace and cache it locally.
    
    Args:
        file_path: Path to the file in the HuggingFace repo
        
    Returns:
        Optional[str]: Local path to the cached file or None if failed
    """
    # Check if file is already cached
    filename = os.path.basename(file_path)
    cache_path = os.path.join(DATA_DIR, filename)
    
    if os.path.exists(cache_path):
        logger.debug(f"Using cached file: {cache_path}")
        return cache_path
    
    try:
        # Download the file
        local_path = hf_hub_download(
            repo_id=REPO_ID,
            filename=file_path,
            repo_type=REPO_TYPE,
            cache_dir=DATA_DIR
        )
        
        logger.info(f"Downloaded file {file_path} to {local_path}")
        return local_path
    except Exception as e:
        logger.error(f"Error downloading file {file_path}: {e}")
        return None

def read_cached_parquet(file_path: str) -> Optional[pd.DataFrame]:
    """
    Read a parquet file from cache or download if not available.
    
    Args:
        file_path: Path to the file in the HuggingFace repo
        
    Returns:
        Optional[pd.DataFrame]: Pandas DataFrame or None if failed
    """
    # First ensure the file is downloaded
    local_path = download_and_cache_file(file_path)
    if not local_path:
        return None
    
    try:
        # Read the parquet file
        df = pd.read_parquet(local_path)
        return df
    except Exception as e:
        logger.error(f"Error reading parquet file {local_path}: {e}")
        return None

def read_cached_json(file_path: str) -> Optional[Dict]:
    """
    Read a JSON file from cache or download if not available.
    
    Args:
        file_path: Path to the file in the HuggingFace repo
        
    Returns:
        Optional[Dict]: Parsed JSON as dictionary or None if failed
    """
    # First ensure the file is downloaded
    local_path = download_and_cache_file(file_path)
    if not local_path:
        return None
    
    try:
        # Read the JSON file
        with open(local_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except Exception as e:
        logger.error(f"Error reading JSON file {local_path}: {e}")
        return None

def read_cached_html(file_path: str) -> Optional[str]:
    """
    Read an HTML file from cache or download if not available.
    
    Args:
        file_path: Path to the file in the HuggingFace repo
        
    Returns:
        Optional[str]: HTML content as string or None if failed
    """
    # First ensure the file is downloaded
    local_path = download_and_cache_file(file_path)
    if not local_path:
        return None
    
    try:
        # Read the HTML file
        with open(local_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return content
    except Exception as e:
        logger.error(f"Error reading HTML file {local_path}: {e}")
        return None

def get_document_html(document_id: str) -> Optional[str]:
    """
    Get the HTML content for a specific document.
    
    Args:
        document_id: Document identifier
        
    Returns:
        Optional[str]: HTML content as string or None if failed
    """
    file_path = f"american_law/html/{document_id}.html"
    return read_cached_html(file_path)

def get_metadata_for_document(document_id: str) -> Optional[Dict]:
    """
    Get metadata for a specific document by searching through metadata files.
    
    Args:
        document_id: Document identifier
        
    Returns:
        Optional[Dict]: Document metadata or None if not found
    """
    # List metadata files
    metadata_files = list_dataset_files("american_law/metadata")
    
    # Try direct approach first (if we know the file)
    try:
        # Metadata files might be organized by state or other criteria
        for metadata_file in metadata_files:
            # Download and read the metadata file
            metadata = read_cached_json(metadata_file)
            if not metadata:
                continue
                
            # Check if document_id exists in this metadata file
            if document_id in metadata:
                return metadata[document_id]
    except Exception as e:
        logger.error(f"Error finding metadata for document {document_id}: {e}")
    
    return None

def batch_download_metadata() -> Dict[str, Dict]:
    """
    Download and cache all metadata files, return combined metadata.
    
    Returns:
        Dict[str, Dict]: Dictionary of document IDs to metadata
    """
    all_metadata = {}
    metadata_files = list_dataset_files("american_law/metadata")
    
    for file_path in metadata_files:
        metadata = read_cached_json(file_path)
        if metadata:
            all_metadata.update(metadata)
    
    logger.info(f"Downloaded metadata for {len(all_metadata)} documents")
    return all_metadata 