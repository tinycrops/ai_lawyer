#!/bin/bash
# Start the American Law Wiki

echo "Starting the American Law Wiki..."

# Make sure the database exists
if [ ! -f "american_law.db" ]; then
    echo "Database not found. Running initialization..."
    python run_processor.py init
fi

# Create directories if they don't exist
mkdir -p templates/wiki
mkdir -p tinycrops_lawyer/wiki

# Make sure the wiki module is properly initialized
if [ ! -f "tinycrops_lawyer/wiki/__init__.py" ]; then
    echo "Creating wiki module..."
    echo '"""Wiki module for TinyCrops AI Lawyer system."""' > tinycrops_lawyer/wiki/__init__.py
fi

# Start the wiki server
python -m tinycrops_lawyer.wiki.app

echo "Wiki server stopped." 