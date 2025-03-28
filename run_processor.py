#!/usr/bin/env python
"""
Command-line utility for running the American Law Dataset processor
with various options and configurations.
"""

import sys
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Import the main CLI function
from tinycrops_lawyer.cli.main import main

if __name__ == "__main__":
    # Print startup message
    print("Starting American Law AI Processor...")
    
    # Run the main CLI function
    sys.exit(main()) 