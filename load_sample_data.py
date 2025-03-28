#!/usr/bin/env python
"""
Load sample data into the American Law Dataset database for testing.
"""

import os
import db_utils
import json
import random
import datetime
from datetime import timedelta

# Clear existing data
os.system('rm american_law_data.db')
db_utils.initialize_database()

# Sample states
states = [
    ('CA', 'California'),
    ('NY', 'New York'),
    ('TX', 'Texas'),
    ('FL', 'Florida'),
    ('IL', 'Illinois'),
    ('PA', 'Pennsylvania'),
    ('OH', 'Ohio'),
    ('GA', 'Georgia'),
    ('NC', 'North Carolina'),
    ('MI', 'Michigan')
]

# Sample places by state
places = {
    'CA': [('CA_SF', 'San Francisco', 'city'), ('CA_LA', 'Los Angeles', 'city'), ('CA_SD', 'San Diego', 'city'), ('CA_SJ', 'San Jose', 'city')],
    'NY': [('NY_NYC', 'New York City', 'city'), ('NY_BUF', 'Buffalo', 'city'), ('NY_ALB', 'Albany', 'city')],
    'TX': [('TX_HOU', 'Houston', 'city'), ('TX_DAL', 'Dallas', 'city'), ('TX_AUS', 'Austin', 'city')],
    'FL': [('FL_MIA', 'Miami', 'city'), ('FL_ORL', 'Orlando', 'city'), ('FL_TPA', 'Tampa', 'city')],
    'IL': [('IL_CHI', 'Chicago', 'city'), ('IL_AUR', 'Aurora', 'city')],
    'PA': [('PA_PHI', 'Philadelphia', 'city'), ('PA_PIT', 'Pittsburgh', 'city')],
    'OH': [('OH_COL', 'Columbus', 'city'), ('OH_CLE', 'Cleveland', 'city')],
    'GA': [('GA_ATL', 'Atlanta', 'city'), ('GA_AUG', 'Augusta', 'city')],
    'NC': [('NC_CHA', 'Charlotte', 'city'), ('NC_RAL', 'Raleigh', 'city')],
    'MI': [('MI_DET', 'Detroit', 'city'), ('MI_GRA', 'Grand Rapids', 'city')]
}

# Sample document types
doc_types = ['municipal_code', 'ordinance', 'regulation', 'statute', 'administrative_code', 'zoning_code', 'building_code', 'health_code']

# Create sample cache directories if they don't exist
os.makedirs('data_cache', exist_ok=True)
os.makedirs('processed_documents', exist_ok=True)

# Add states
for state_code, state_name in states:
    db_utils.add_state(state_code, state_name)

# Add places
for state_code, state_places in places.items():
    for place_id, place_name, place_type in state_places:
        db_utils.add_place(place_id, state_code, place_name, place_type)

# Generate sample documents
doc_count = 0
for state_code, state_places in places.items():
    for place_id, place_name, place_type in state_places:
        # Random number of documents per place (10-50)
        num_docs = random.randint(10, 50)
        for i in range(num_docs):
            doc_count += 1
            document_id = f'doc{doc_count:06d}'
            document_type = random.choice(doc_types)
            
            # Generate a random date in the last 5 years
            days_ago = random.randint(0, 365 * 5)
            document_date = (datetime.datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
            
            # Create placeholder file paths
            html_path = f'data_cache/{document_id}.html'
            metadata_path = f'data_cache/{document_id}_meta.json'
            
            # Create placeholder files with minimal content
            with open(html_path, 'w') as f:
                f.write(f'<html><body><h1>Sample Document {document_id}</h1><p>This is a sample {document_type} from {place_name}, {state_code}.</p></body></html>')
            
            with open(metadata_path, 'w') as f:
                json.dump({
                    'document_id': document_id,
                    'document_type': document_type,
                    'state': state_code,
                    'place': place_name,
                    'date': document_date
                }, f)
            
            # Add document to database
            db_utils.add_document(document_id, state_code, place_id, document_type, document_date, html_path, metadata_path)

# Mark some documents as processed (randomly 30-70% of documents)
processed_count = 0
error_count = 0
for i in range(1, doc_count + 1):
    document_id = f'doc{i:06d}'
    if random.random() < 0.5:  # 50% chance of being processed
        processed_count += 1
        processed_path = f'processed_documents/{document_id}.json'
        
        # Simulate a processed document
        with open(processed_path, 'w') as f:
            json.dump({
                'document_id': document_id,
                'processed_data': {'sample': 'data'},
                'processing_time': random.uniform(0.5, 5.0)
            }, f)
        
        # 10% chance of processing error
        error = None
        if random.random() < 0.1:
            error_count += 1
            error = 'Sample processing error'
        
        # Mark as processed
        db_utils.mark_document_processed(document_id, processed_path, random.uniform(0.5, 5.0), error)

# Add some processing runs
start_date = datetime.datetime.now() - timedelta(days=30)
for i in range(10):
    # Create a run date between start_date and now
    run_date = start_date + timedelta(days=i*3)
    
    # Start a run
    run_id = db_utils.start_processing_run('gemini-2.0-flash', f'Sample batch run {i+1}')
    
    # Simulate processing between 5-20 documents per run
    docs_in_run = random.randint(5, 20)
    errors_in_run = random.randint(0, 3)
    
    # End the run with a random status
    status = random.choice(['completed', 'completed', 'completed', 'error'])
    
    # Set the end date a few hours after start
    end_date = run_date + timedelta(hours=random.randint(1, 8))
    
    # Use direct SQL to set the exact dates for the run
    conn = db_utils.get_connection()
    cursor = conn.cursor()
    cursor.execute(
        'UPDATE processing_runs SET start_time = ?, end_time = ?, status = ?, documents_processed = ?, errors_count = ? WHERE run_id = ?',
        (run_date.strftime('%Y-%m-%d %H:%M:%S'), end_date.strftime('%Y-%m-%d %H:%M:%S'), 
         status, docs_in_run, errors_in_run, run_id)
    )
    conn.commit()
    conn.close()

print(f'Added {doc_count} sample documents, marked {processed_count} as processed with {error_count} errors')
print(f'Created 10 sample processing runs')
print('Sample data loaded successfully')

if __name__ == '__main__':
    pass  # All code executes on import 