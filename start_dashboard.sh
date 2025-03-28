#!/bin/bash
# Start the American Law Dataset Dashboard

echo "Starting the American Law Dataset Dashboard..."

# Make sure the database exists
if [ ! -f "american_law.db" ]; then
    echo "Database not found. Running initialization..."
    python run_processor.py init
fi

# Start the dashboard
python -m tinycrops_lawyer.dashboard.app

echo "Dashboard stopped." 