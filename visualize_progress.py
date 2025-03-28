#!/usr/bin/env python
"""
Visualization tool for tracking progress of the American Law Dataset parsing.
Generates HTML reports showing jurisdiction coverage, document types, and parsing status.
"""

import os
import json
import sqlite3
from pathlib import Path
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from jinja2 import Template

# Constants
DB_PATH = "american_law_data.db"
OUTPUT_DIR = "reports"
REPORT_DATE = datetime.now().strftime("%Y-%m-%d")

# Create output directory
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Database connection
def get_db_connection():
    """Return a connection to the SQLite database"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def generate_state_chart(states_data, output_file="state_coverage.png"):
    """Generate a chart showing document counts by state"""
    # Convert to dataframe
    df = pd.DataFrame(states_data)
    
    # Sort by document count
    df = df.sort_values(by="document_count", ascending=False).head(15)
    
    # Create stacked bar chart for processed vs unprocessed
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Calculate unprocessed
    df['unprocessed'] = df['document_count'] - df['processed_count']
    
    # Create the stacked bars
    ax.bar(df['state_code'], df['processed_count'], label='Processed')
    ax.bar(df['state_code'], df['unprocessed'], bottom=df['processed_count'], label='Unprocessed')
    
    # Add percentage labels
    for i, row in df.iterrows():
        if row['document_count'] > 0:
            percentage = (row['processed_count'] / row['document_count']) * 100
            ax.text(i, row['document_count'] + 50, f"{percentage:.1f}%", 
                    ha='center', va='bottom', rotation=0)
    
    # Add labels and title
    ax.set_xlabel('State')
    ax.set_ylabel('Document Count')
    ax.set_title('Document Counts by State (Top 15)')
    ax.legend()
    
    # Save chart
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, output_file))
    plt.close()
    
    return output_file

def generate_document_type_chart(doc_types_data, output_file="document_types.png"):
    """Generate a chart showing document counts by type"""
    # Convert to dataframe
    df = pd.DataFrame(doc_types_data)
    
    # Filter out null document types
    df = df[df['document_type'].notna()]
    
    # Sort by document count
    df = df.sort_values(by="count", ascending=False)
    
    # Create stacked bar chart for processed vs unprocessed
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Calculate unprocessed
    df['unprocessed'] = df['count'] - df['processed_count']
    
    # Create the stacked bars
    ax.bar(df['document_type'], df['processed_count'], label='Processed')
    ax.bar(df['document_type'], df['unprocessed'], bottom=df['processed_count'], label='Unprocessed')
    
    # Add percentage labels
    for i, row in df.iterrows():
        if row['count'] > 0:
            percentage = (row['processed_count'] / row['count']) * 100
            ax.text(i, row['count'] + 5, f"{percentage:.1f}%", 
                    ha='center', va='bottom', rotation=90)
    
    # Add labels and title
    ax.set_xlabel('Document Type')
    ax.set_ylabel('Count')
    ax.set_title('Document Counts by Type')
    ax.legend()
    
    # Save chart
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, output_file))
    plt.close()
    
    return output_file

def generate_place_tables(top_places_by_state):
    """Generate HTML tables for top places in each state"""
    # Create HTML for each state
    state_tables = []
    
    for state_data in top_places_by_state:
        state_code = state_data['state_code']
        state_name = state_data['state_name']
        places = state_data['places']
        
        # Generate HTML table for this state
        places_html = f"""
        <div class="state-section">
            <h3>{state_name} ({state_code})</h3>
            <table class="table table-striped">
                <thead>
                    <tr>
                        <th>Place</th>
                        <th>Documents</th>
                        <th>Processed</th>
                        <th>Completion</th>
                    </tr>
                </thead>
                <tbody>
        """
        
        for place in places:
            completion = (place['processed_count'] / place['document_count']) * 100 if place['document_count'] > 0 else 0
            
            places_html += f"""
                <tr>
                    <td>{place['place_name']}</td>
                    <td>{place['document_count']}</td>
                    <td>{place['processed_count']}</td>
                    <td>
                        <div class="progress">
                            <div class="progress-bar" role="progressbar" 
                                 style="width: {completion}%;" 
                                 aria-valuenow="{completion}" aria-valuemin="0" aria-valuemax="100">
                                {completion:.1f}%
                            </div>
                        </div>
                    </td>
                </tr>
            """
        
        places_html += """
                </tbody>
            </table>
        </div>
        """
        
        state_tables.append(places_html)
    
    return "\n".join(state_tables)

def generate_html_report(report_data):
    """Generate HTML report from the report data"""
    # Generate charts
    state_chart = generate_state_chart(report_data['states'])
    doc_type_chart = generate_document_type_chart(report_data['document_types'])
    
    # Generate place tables
    place_tables = generate_place_tables(report_data['top_places_by_state'])
    
    # Calculate overall stats
    total_documents = sum(state['document_count'] for state in report_data['states'])
    processed_documents = sum(state['processed_count'] for state in report_data['states'])
    completion_percentage = (processed_documents / total_documents) * 100 if total_documents > 0 else 0
    
    # Read HTML template
    template_str = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>American Law Dataset Processing Report</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/css/bootstrap.min.css" rel="stylesheet">
        <style>
            body { padding: 20px; }
            .state-section { margin-bottom: 30px; }
            .progress { height: 20px; }
            .chart-container { margin: 20px 0; text-align: center; }
            .summary-box { padding: 20px; margin-bottom: 20px; border-radius: 5px; }
            .bg-light-blue { background-color: #e3f2fd; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1 class="mb-4">American Law Dataset Processing Report</h1>
            <p class="text-muted">Generated on {{ report_date }}</p>
            
            <div class="row">
                <div class="col-md-4">
                    <div class="summary-box bg-light-blue">
                        <h3>Overall Progress</h3>
                        <div class="progress mb-3">
                            <div class="progress-bar" role="progressbar" 
                                 style="width: {{ completion_percentage }}%;" 
                                 aria-valuenow="{{ completion_percentage }}" aria-valuemin="0" aria-valuemax="100">
                                {{ completion_percentage|round(1) }}%
                            </div>
                        </div>
                        <p>{{ processed_documents }} of {{ total_documents }} documents processed</p>
                    </div>
                </div>
                <div class="col-md-4">
                    <div class="summary-box bg-light-blue">
                        <h3>States Coverage</h3>
                        <p>{{ states_count }} states with legal documents</p>
                        <p>Top state: {{ top_state_name }} ({{ top_state_count }} documents)</p>
                    </div>
                </div>
                <div class="col-md-4">
                    <div class="summary-box bg-light-blue">
                        <h3>Document Types</h3>
                        <p>{{ document_types_count }} different document types</p>
                        <p>Most common: {{ top_document_type }} ({{ top_document_type_count }} documents)</p>
                    </div>
                </div>
            </div>
            
            <div class="row mt-4">
                <div class="col-md-6">
                    <div class="chart-container">
                        <h2>State Coverage</h2>
                        <img src="{{ state_chart }}" alt="State Coverage Chart" class="img-fluid">
                    </div>
                </div>
                <div class="col-md-6">
                    <div class="chart-container">
                        <h2>Document Types</h2>
                        <img src="{{ doc_type_chart }}" alt="Document Types Chart" class="img-fluid">
                    </div>
                </div>
            </div>
            
            <h2 class="mt-5 mb-4">Top Places by State</h2>
            {{ place_tables|safe }}
        </div>
        
        <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/js/bootstrap.bundle.min.js"></script>
    </body>
    </html>
    """
    
    # Create template
    template = Template(template_str)
    
    # Get additional stats
    states_count = len(report_data['states'])
    document_types_count = len(report_data['document_types'])
    
    # Find top state
    top_state = max(report_data['states'], key=lambda x: x['document_count'])
    top_state_name = top_state['state_name']
    top_state_count = top_state['document_count']
    
    # Find top document type
    valid_doc_types = [dt for dt in report_data['document_types'] if dt['document_type']]
    if valid_doc_types:
        top_doc_type = max(valid_doc_types, key=lambda x: x['count'])
        top_document_type = top_doc_type['document_type']
        top_document_type_count = top_doc_type['count']
    else:
        top_document_type = "Unknown"
        top_document_type_count = 0
    
    # Render template
    html = template.render(
        report_date=REPORT_DATE,
        total_documents=total_documents,
        processed_documents=processed_documents,
        completion_percentage=completion_percentage,
        states_count=states_count,
        document_types_count=document_types_count,
        top_state_name=top_state_name,
        top_state_count=top_state_count,
        top_document_type=top_document_type,
        top_document_type_count=top_document_type_count,
        state_chart=state_chart,
        doc_type_chart=doc_type_chart,
        place_tables=place_tables
    )
    
    # Save HTML report
    report_file = os.path.join(OUTPUT_DIR, f"progress_report_{REPORT_DATE}.html")
    with open(report_file, 'w') as f:
        f.write(html)
    
    return report_file

def main():
    """Generate visualization report"""
    conn = get_db_connection()
    
    # Get state statistics
    states = conn.execute(
        """
        SELECT state_code, state_name, document_count, processed_count,
               (CAST(processed_count AS FLOAT) / CAST(document_count AS FLOAT)) * 100 AS completion_percentage
        FROM states
        ORDER BY document_count DESC
        """
    ).fetchall()
    
    # Get document type statistics
    doc_types = conn.execute(
        """
        SELECT document_type, COUNT(*) as count,
               SUM(CASE WHEN is_processed = 1 THEN 1 ELSE 0 END) as processed_count,
               (SUM(CASE WHEN is_processed = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as completion_percentage
        FROM jurisdiction_documents
        GROUP BY document_type
        ORDER BY count DESC
        """
    ).fetchall()
    
    # Get place statistics for top states
    top_places = []
    for state in states[:5]:  # Top 5 states
        places = conn.execute(
            """
            SELECT place_name, document_count, processed_count,
                   (CAST(processed_count AS FLOAT) / CAST(document_count AS FLOAT)) * 100 AS completion_percentage
            FROM places
            WHERE state_code = ?
            ORDER BY document_count DESC
            LIMIT 10
            """,
            (state['state_code'],)
        ).fetchall()
        
        top_places.append({
            'state_code': state['state_code'],
            'state_name': state['state_name'],
            'places': [dict(place) for place in places]
        })
    
    # Create report data
    report_data = {
        'states': [dict(state) for state in states],
        'document_types': [dict(doc_type) for doc_type in doc_types],
        'top_places_by_state': top_places
    }
    
    # Generate HTML report
    report_file = generate_html_report(report_data)
    print(f"Generated report: {report_file}")
    
    conn.close()

if __name__ == "__main__":
    main() 