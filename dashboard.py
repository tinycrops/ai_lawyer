#!/usr/bin/env python
"""
Interactive dashboard for monitoring the American Law Dataset processing progress.
Provides real-time updates, metrics, and visualizations of the processing status.
"""

import os
import json
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import time
import io
import base64
from flask import Flask, render_template, jsonify, request, redirect, url_for
import db_utils
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='dashboard.log'
)
logger = logging.getLogger('dashboard')

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = 'american-law-dashboard'
app.config['TEMPLATES_AUTO_RELOAD'] = True

# Create templates directory if it doesn't exist
os.makedirs('templates', exist_ok=True)

# Create a placeholder HTML template for the dashboard
with open('templates/index.html', 'w') as f:
    f.write("""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>American Law Dataset Dashboard</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/css/bootstrap.min.css" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { padding: 20px; }
        .card { margin-bottom: 20px; }
        .progress { height: 25px; }
        .progress-bar { font-size: 14px; line-height: 25px; }
        .stats-card { min-height: 200px; }
        .chart-container { height: 300px; }
        .refresh-btn { position: absolute; top: 10px; right: 10px; }
        .summary-box { 
            border-radius: 5px; 
            padding: 15px; 
            margin-bottom: 20px;
            background-color: #f8f9fa;
            border-left: 5px solid #0d6efd;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="d-flex justify-content-between align-items-center mb-4">
            <h1>American Law Dataset Processing Dashboard</h1>
            <div>
                <button id="refreshBtn" class="btn btn-primary">Refresh Data</button>
                <a href="/run-processor" class="btn btn-success ms-2">Run Processor</a>
            </div>
        </div>
        
        <div class="row mb-4">
            <div class="col-md-4">
                <div class="summary-box">
                    <h3>Overall Progress</h3>
                    <div class="progress mb-3">
                        <div id="overallProgress" class="progress-bar" role="progressbar" 
                             style="width: 0%;" 
                             aria-valuenow="0" aria-valuemin="0" aria-valuemax="100">
                            0%
                        </div>
                    </div>
                    <p id="overallStats">0 of 0 documents processed</p>
                </div>
            </div>
            <div class="col-md-4">
                <div class="summary-box">
                    <h3>States Coverage</h3>
                    <p id="statesCount">0 states with legal documents</p>
                    <p id="topState">Top state: N/A (0 documents)</p>
                </div>
            </div>
            <div class="col-md-4">
                <div class="summary-box">
                    <h3>Document Types</h3>
                    <p id="docTypesCount">0 different document types</p>
                    <p id="topDocType">Most common: N/A (0 documents)</p>
                </div>
            </div>
        </div>
        
        <div class="row mb-4">
            <div class="col-md-6">
                <div class="card">
                    <div class="card-header">
                        <h5>State Coverage</h5>
                    </div>
                    <div class="card-body">
                        <div class="chart-container">
                            <canvas id="stateChart"></canvas>
                        </div>
                    </div>
                </div>
            </div>
            <div class="col-md-6">
                <div class="card">
                    <div class="card-header">
                        <h5>Document Types</h5>
                    </div>
                    <div class="card-body">
                        <div class="chart-container">
                            <canvas id="docTypeChart"></canvas>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="row mb-4">
            <div class="col-md-6">
                <div class="card">
                    <div class="card-header">
                        <h5>Processing History</h5>
                    </div>
                    <div class="card-body">
                        <div class="chart-container">
                            <canvas id="historyChart"></canvas>
                        </div>
                    </div>
                </div>
            </div>
            <div class="col-md-6">
                <div class="card">
                    <div class="card-header">
                        <h5>Recent Processing Runs</h5>
                    </div>
                    <div class="card-body">
                        <div id="recentRuns">
                            <div class="text-center">
                                <div class="spinner-border text-primary" role="status">
                                    <span class="visually-hidden">Loading...</span>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="row">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h5>Top States by Completion</h5>
                    </div>
                    <div class="card-body">
                        <div id="statesTable">
                            <div class="text-center">
                                <div class="spinner-border text-primary" role="status">
                                    <span class="visually-hidden">Loading...</span>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
    
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/js/bootstrap.bundle.min.js"></script>
    <script>
        // Chart objects
        let stateChart, docTypeChart, historyChart;
        
        // Fetch dashboard data
        function fetchDashboardData() {
            fetch('/api/dashboard-data')
                .then(response => response.json())
                .then(data => {
                    updateDashboard(data);
                })
                .catch(error => {
                    console.error('Error fetching dashboard data:', error);
                });
        }
        
        // Update dashboard with new data
        function updateDashboard(data) {
            // Update overall progress
            const overallProgress = document.getElementById('overallProgress');
            overallProgress.style.width = data.overall_percentage + '%';
            overallProgress.innerText = data.overall_percentage.toFixed(1) + '%';
            overallProgress.setAttribute('aria-valuenow', data.overall_percentage);
            
            document.getElementById('overallStats').innerText = 
                `${data.processed_documents} of ${data.total_documents} documents processed`;
            
            // Update states info
            document.getElementById('statesCount').innerText = 
                `${data.states_count} states with legal documents`;
            document.getElementById('topState').innerText = 
                `Top state: ${data.top_state.name} (${data.top_state.count} documents)`;
            
            // Update doc types info
            document.getElementById('docTypesCount').innerText = 
                `${data.document_types_count} different document types`;
            document.getElementById('topDocType').innerText = 
                `Most common: ${data.top_document_type.name} (${data.top_document_type.count} documents)`;
            
            // Update state chart
            updateStateChart(data.top_states);
            
            // Update doc type chart
            updateDocTypeChart(data.top_document_types);
            
            // Update history chart
            updateHistoryChart(data.processing_history);
            
            // Update recent runs table
            updateRecentRuns(data.recent_runs);
            
            // Update states table
            updateStatesTable(data.top_states);
        }
        
        // Update state chart
        function updateStateChart(states) {
            const ctx = document.getElementById('stateChart').getContext('2d');
            
            const labels = states.map(state => state.state_code);
            const processedData = states.map(state => state.processed_count);
            const unprocessedData = states.map(state => state.document_count - state.processed_count);
            
            if (stateChart) {
                stateChart.data.labels = labels;
                stateChart.data.datasets[0].data = processedData;
                stateChart.data.datasets[1].data = unprocessedData;
                stateChart.update();
            } else {
                stateChart = new Chart(ctx, {
                    type: 'bar',
                    data: {
                        labels: labels,
                        datasets: [
                            {
                                label: 'Processed',
                                data: processedData,
                                backgroundColor: 'rgba(40, 167, 69, 0.7)',
                                borderColor: 'rgba(40, 167, 69, 1)',
                                borderWidth: 1
                            },
                            {
                                label: 'Unprocessed',
                                data: unprocessedData,
                                backgroundColor: 'rgba(220, 53, 69, 0.7)',
                                borderColor: 'rgba(220, 53, 69, 1)',
                                borderWidth: 1
                            }
                        ]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: {
                            x: {
                                stacked: true,
                                title: {
                                    display: true,
                                    text: 'State'
                                }
                            },
                            y: {
                                stacked: true,
                                title: {
                                    display: true,
                                    text: 'Documents'
                                }
                            }
                        }
                    }
                });
            }
        }
        
        // Update document type chart
        function updateDocTypeChart(docTypes) {
            const ctx = document.getElementById('docTypeChart').getContext('2d');
            
            const labels = docTypes.map(dt => dt.document_type || 'Unknown');
            const processedData = docTypes.map(dt => dt.processed_count);
            const unprocessedData = docTypes.map(dt => dt.count - dt.processed_count);
            
            if (docTypeChart) {
                docTypeChart.data.labels = labels;
                docTypeChart.data.datasets[0].data = processedData;
                docTypeChart.data.datasets[1].data = unprocessedData;
                docTypeChart.update();
            } else {
                docTypeChart = new Chart(ctx, {
                    type: 'bar',
                    data: {
                        labels: labels,
                        datasets: [
                            {
                                label: 'Processed',
                                data: processedData,
                                backgroundColor: 'rgba(40, 167, 69, 0.7)',
                                borderColor: 'rgba(40, 167, 69, 1)',
                                borderWidth: 1
                            },
                            {
                                label: 'Unprocessed',
                                data: unprocessedData,
                                backgroundColor: 'rgba(220, 53, 69, 0.7)',
                                borderColor: 'rgba(220, 53, 69, 1)',
                                borderWidth: 1
                            }
                        ]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: {
                            x: {
                                stacked: true,
                                title: {
                                    display: true,
                                    text: 'Document Type'
                                }
                            },
                            y: {
                                stacked: true,
                                title: {
                                    display: true,
                                    text: 'Documents'
                                }
                            }
                        }
                    }
                });
            }
        }
        
        // Update history chart
        function updateHistoryChart(history) {
            const ctx = document.getElementById('historyChart').getContext('2d');
            
            const labels = history.dates;
            const processedData = history.processed_counts;
            const errorsData = history.error_counts;
            
            if (historyChart) {
                historyChart.data.labels = labels;
                historyChart.data.datasets[0].data = processedData;
                historyChart.data.datasets[1].data = errorsData;
                historyChart.update();
            } else {
                historyChart = new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: labels,
                        datasets: [
                            {
                                label: 'Documents Processed',
                                data: processedData,
                                borderColor: 'rgba(40, 167, 69, 1)',
                                backgroundColor: 'rgba(40, 167, 69, 0.1)',
                                borderWidth: 2,
                                fill: true
                            },
                            {
                                label: 'Errors',
                                data: errorsData,
                                borderColor: 'rgba(220, 53, 69, 1)',
                                backgroundColor: 'rgba(220, 53, 69, 0.1)',
                                borderWidth: 2,
                                fill: true
                            }
                        ]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: {
                            x: {
                                title: {
                                    display: true,
                                    text: 'Date'
                                }
                            },
                            y: {
                                title: {
                                    display: true,
                                    text: 'Count'
                                }
                            }
                        }
                    }
                });
            }
        }
        
        // Update recent runs table
        function updateRecentRuns(runs) {
            let html = '<table class="table table-hover">';
            html += '<thead><tr><th>ID</th><th>Date</th><th>Model</th><th>Status</th><th>Processed</th><th>Errors</th></tr></thead>';
            html += '<tbody>';
            
            runs.forEach(run => {
                const statusClass = run.status === 'completed' ? 'text-success' : 
                                     run.status === 'error' ? 'text-danger' : 'text-warning';
                                     
                html += `<tr>
                    <td>${run.run_id}</td>
                    <td>${run.start_time}</td>
                    <td>${run.model_name}</td>
                    <td class="${statusClass}">${run.status}</td>
                    <td>${run.documents_processed}</td>
                    <td>${run.errors_count}</td>
                </tr>`;
            });
            
            html += '</tbody></table>';
            document.getElementById('recentRuns').innerHTML = html;
        }
        
        // Update states table
        function updateStatesTable(states) {
            let html = '<table class="table table-hover">';
            html += '<thead><tr><th>State</th><th>Documents</th><th>Processed</th><th>Progress</th></tr></thead>';
            html += '<tbody>';
            
            states.forEach(state => {
                const percentage = (state.processed_count / state.document_count) * 100;
                
                html += `<tr>
                    <td>${state.state_name} (${state.state_code})</td>
                    <td>${state.document_count}</td>
                    <td>${state.processed_count}</td>
                    <td>
                        <div class="progress">
                            <div class="progress-bar" role="progressbar" 
                                 style="width: ${percentage}%;" 
                                 aria-valuenow="${percentage}" aria-valuemin="0" aria-valuemax="100">
                                ${percentage.toFixed(1)}%
                            </div>
                        </div>
                    </td>
                </tr>`;
            });
            
            html += '</tbody></table>';
            document.getElementById('statesTable').innerHTML = html;
        }
        
        // Initialize dashboard
        document.addEventListener('DOMContentLoaded', function() {
            fetchDashboardData();
            
            // Set up refresh button
            document.getElementById('refreshBtn').addEventListener('click', function() {
                fetchDashboardData();
            });
            
            // Auto-refresh every 30 seconds
            setInterval(fetchDashboardData, 30000);
        });
    </script>
</body>
</html>
    """)

def generate_state_data():
    """Get state data for the dashboard"""
    try:
        stats = db_utils.get_processing_statistics()
        return stats['state_stats']
    except Exception as e:
        logger.error(f"Error generating state data: {e}")
        return []

def generate_document_type_data():
    """Get document type data for the dashboard"""
    try:
        stats = db_utils.get_processing_statistics()
        return stats['document_types']
    except Exception as e:
        logger.error(f"Error generating document type data: {e}")
        return []

def generate_processing_history():
    """Generate processing history data for the dashboard"""
    try:
        conn = db_utils.get_connection(read_only=True)
        cursor = conn.cursor()
        
        # Get processing runs grouped by date
        cursor.execute(
            """
            SELECT DATE(start_time) as date, 
                   SUM(documents_processed) as processed_count,
                   SUM(errors_count) as error_count
            FROM processing_runs
            GROUP BY DATE(start_time)
            ORDER BY date DESC
            LIMIT 14
            """
        )
        results = cursor.fetchall()
        conn.close()
        
        # Reverse to get chronological order
        results.reverse()
        
        # Extract data for chart
        dates = [result['date'] for result in results]
        processed_counts = [result['processed_count'] for result in results]
        error_counts = [result['error_count'] for result in results]
        
        return {
            'dates': dates,
            'processed_counts': processed_counts,
            'error_counts': error_counts
        }
    except Exception as e:
        logger.error(f"Error generating processing history: {e}")
        return {'dates': [], 'processed_counts': [], 'error_counts': []}

@app.route('/')
def index():
    """Dashboard home page"""
    return render_template('index.html')

@app.route('/api/dashboard-data')
def dashboard_data():
    """API endpoint for dashboard data"""
    try:
        # Get processing statistics
        stats = db_utils.get_processing_statistics()
        
        # Get processing history
        history = generate_processing_history()
        
        # Find top state
        top_state = {'name': 'N/A', 'count': 0}
        if stats['state_stats']:
            state = stats['state_stats'][0]
            top_state = {'name': state['state_name'], 'count': state['document_count']}
        
        # Find top document type
        top_document_type = {'name': 'Unknown', 'count': 0}
        if stats['document_types']:
            for doc_type in stats['document_types']:
                if doc_type['document_type']:
                    top_document_type = {'name': doc_type['document_type'], 'count': doc_type['count']}
                    break
        
        dashboard_data = {
            'total_documents': stats['total_documents'],
            'processed_documents': stats['processed_documents'],
            'overall_percentage': stats['overall_percentage'],
            'states_count': stats['states_count'],
            'document_types_count': len(stats['document_types']),
            'top_state': top_state,
            'top_document_type': top_document_type,
            'top_states': stats['state_stats'][:10],
            'top_document_types': stats['document_types'][:10],
            'recent_runs': stats['recent_runs'][:5],
            'processing_history': history
        }
        
        return jsonify(dashboard_data)
    except Exception as e:
        logger.error(f"Error generating dashboard data: {e}")
        return jsonify({'error': str(e)})

@app.route('/run-processor')
def run_processor():
    """Run processor from the dashboard"""
    try:
        # Import here to avoid circular imports
        import subprocess
        
        # Run processor with default settings
        subprocess.Popen(["python", "run_processor.py", "process", "--limit", "10"])
        
        # Redirect back to dashboard
        return redirect(url_for('index'))
    except Exception as e:
        logger.error(f"Error running processor: {e}")
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    # Make sure the database is initialized
    if not os.path.exists('american_law_data.db'):
        print("Initializing database...")
        db_utils.initialize_database()
    
    # Run the Flask application
    app.run(debug=True, host='0.0.0.0', port=5000) 