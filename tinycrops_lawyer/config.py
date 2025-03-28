"""
Configuration settings for the TinyCrops AI Lawyer system.
Contains constants, paths, and configuration utilities.
"""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# Base directories
ROOT_DIR = Path(__file__).parent.parent
DATA_DIR = ROOT_DIR / "data_cache"
OUTPUT_DIR = ROOT_DIR / "processed_documents"
SCHEMA_CACHE_DIR = ROOT_DIR / "schema_cache"
DB_BACKUP_DIR = ROOT_DIR / "db_backups"
REPORTS_DIR = ROOT_DIR / "reports"

# Database settings
DB_PATH = ROOT_DIR / "american_law.db"

# Gemini API
DEFAULT_MODEL = "gemini-2.0-flash"
DEFAULT_TEMPERATURE = 0.2
DEFAULT_MAX_OUTPUT_TOKENS = 8192

# Processing settings
DEFAULT_BATCH_SIZE = 10

# Ensure directories exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(SCHEMA_CACHE_DIR, exist_ok=True)
os.makedirs(DB_BACKUP_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(ROOT_DIR / 'processor.log'),
        logging.StreamHandler()
    ]
)

def get_gemini_api_key():
    """
    Get the Gemini API key from environment variables.
    
    Checks for GEMINI_API_KEY first, then GOOGLE_API_KEY as fallback.
    
    Returns:
        str: The API key or None if not found
    """
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        logging.warning("No Gemini API key found. Set GEMINI_API_KEY environment variable.")
    return api_key 