"""
Interactive dashboard for monitoring the American Law Dataset processing progress.
Provides real-time updates, metrics, and visualizations of the processing status.
"""

import os
import sys
import json
import logging
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import time
import io
import base64
import subprocess
from pathlib import Path

from flask import Flask, render_template, jsonify, request, redirect, url_for

from tinycrops_lawyer.database import manager as db_manager
from tinycrops_lawyer.config import ROOT_DIR, REPORTS_DIR

# Initialize logger
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__, template_folder=Path(__file__).parent / 'templates')
app.config['SECRET_KEY'] = 'american-law-dashboard'
app.config['TEMPLATES_AUTO_RELOAD'] = True

def create_template_if_missing():
    """Create a placeholder template if it doesn't exist"""
    template_dir = Path(__file__).parent / 'templates'
    os.makedirs(template_dir, exist_ok=True)
    
    template_path = template_dir / 'index.html'
    
    if not template_path.exists():
        with open(template_path, 'w') as f:
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
            const percentage = data.overall_stats.percentage.toFixed(2);
            overallProgress.style.width = `${percentage}%`;
            overallProgress.innerText = `${percentage}%`;
            overallProgress.setAttribute('aria-valuenow', percentage);
            
            document.getElementById('overallStats').innerText = 
                `${data.overall_stats.processed_documents} of ${data.overall_stats.total_documents} documents processed`;
            
            document.getElementById('statesCount').innerText = 
                `${data.state_data.states_count} states with legal documents`;
            
            if (data.state_data.top_states && data.state_data.top_states.length > 0) {
                const topState = data.state_data.top_states[0];
                document.getElementById('topState').innerText = 
                    `Top state: ${topState.state_name} (${topState.document_count} documents)`;
            }
            
            document.getElementById('docTypesCount').innerText = 
                `${data.document_types.length} different document types`;
            
            if (data.document_types && data.document_types.length > 0) {
                const topType = data.document_types[0];
                document.getElementById('topDocType').innerText = 
                    `Most common: ${topType.document_type || 'Unknown'} (${topType.count} documents)`;
            }
            
            // Update charts
            updateStateChart(data.state_data);
            updateDocTypeChart(data.document_types);
            updateHistoryChart(data.processing_history);
            
            // Update recent runs table
            updateRecentRuns(data.recent_runs);
            
            // Update states table
            updateStatesTable(data.state_data.top_states);
        }
        
        // Update state chart
        function updateStateChart(stateData) {
            const ctx = document.getElementById('stateChart').getContext('2d');
            
            // Get top 10 states by document count
            const states = stateData.top_states.slice(0, 10);
            const labels = states.map(s => s.state_code);
            const processed = states.map(s => s.processed_count);
            const unprocessed = states.map(s => s.document_count - s.processed_count);
            
            if (stateChart) {
                stateChart.data.labels = labels;
                stateChart.data.datasets[0].data = processed;
                stateChart.data.datasets[1].data = unprocessed;
                stateChart.update();
            } else {
                stateChart = new Chart(ctx, {
                    type: 'bar',
                    data: {
                        labels: labels,
                        datasets: [
                            {
                                label: 'Processed',
                                data: processed,
                                backgroundColor: 'rgba(75, 192, 192, 0.8)',
                                borderColor: 'rgba(75, 192, 192, 1)',
                                borderWidth: 1
                            },
                            {
                                label: 'Unprocessed',
                                data: unprocessed,
                                backgroundColor: 'rgba(201, 203, 207, 0.8)',
                                borderColor: 'rgba(201, 203, 207, 1)',
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
                            },
                            y: {
                                stacked: true,
                                beginAtZero: true
                            }
                        }
                    }
                });
            }
        }
        
        // Update document type chart
        function updateDocTypeChart(docTypes) {
            const ctx = document.getElementById('docTypeChart').getContext('2d');
            
            // Get top 5 document types
            const types = docTypes.slice(0, 5);
            const labels = types.map(t => t.document_type || 'Unknown');
            const counts = types.map(t => t.count);
            const colors = [
                'rgba(54, 162, 235, 0.8)',
                'rgba(255, 99, 132, 0.8)',
                'rgba(255, 206, 86, 0.8)',
                'rgba(75, 192, 192, 0.8)',
                'rgba(153, 102, 255, 0.8)'
            ];
            
            if (docTypeChart) {
                docTypeChart.data.labels = labels;
                docTypeChart.data.datasets[0].data = counts;
                docTypeChart.update();
            } else {
                docTypeChart = new Chart(ctx, {
                    type: 'pie',
                    data: {
                        labels: labels,
                        datasets: [{
                            data: counts,
                            backgroundColor: colors,
                            borderColor: colors.map(c => c.replace('0.8', '1')),
                            borderWidth: 1
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {
                            legend: {
                                position: 'right',
                            }
                        }
                    }
                });
            }
        }
        
        // Update processing history chart
        function updateHistoryChart(history) {
            const ctx = document.getElementById('historyChart').getContext('2d');
            
            const dates = history.map(h => h.date);
            const processed = history.map(h => h.processed);
            
            if (historyChart) {
                historyChart.data.labels = dates;
                historyChart.data.datasets[0].data = processed;
                historyChart.update();
            } else {
                historyChart = new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: dates,
                        datasets: [{
                            label: 'Documents Processed',
                            data: processed,
                            fill: true,
                            backgroundColor: 'rgba(54, 162, 235, 0.2)',
                            borderColor: 'rgba(54, 162, 235, 1)',
                            tension: 0.1
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: {
                            y: {
                                beginAtZero: true
                            }
                        }
                    }
                });
            }
        }
        
        // Update recent runs table
        function updateRecentRuns(runs) {
            const container = document.getElementById('recentRuns');
            
            if (!runs || runs.length === 0) {
                container.innerHTML = '<p class="text-center">No recent processing runs</p>';
                return;
            }
            
            let html = '<table class="table table-striped">';
            html += '<thead><tr><th>Run ID</th><th>Start Time</th><th>Status</th><th>Processed</th><th>Errors</th><th>Model</th></tr></thead>';
            html += '<tbody>';
            
            runs.forEach(run => {
                const status = run.status === 'completed' 
                    ? '<span class="badge bg-success">Completed</span>' 
                    : run.status === 'error'
                        ? '<span class="badge bg-danger">Error</span>'
                        : '<span class="badge bg-warning">Running</span>';
                
                html += `<tr>
                    <td>${run.run_id}</td>
                    <td>${new Date(run.start_time).toLocaleString()}</td>
                    <td>${status}</td>
                    <td>${run.documents_processed}</td>
                    <td>${run.errors_count}</td>
                    <td>${run.model_name}</td>
                </tr>`;
            });
            
            html += '</tbody></table>';
            container.innerHTML = html;
        }
        
        // Update states table
        function updateStatesTable(states) {
            const container = document.getElementById('statesTable');
            
            if (!states || states.length === 0) {
                container.innerHTML = '<p class="text-center">No states data available</p>';
                return;
            }
            
            let html = '<table class="table table-striped">';
            html += '<thead><tr><th>State</th><th>Code</th><th>Documents</th><th>Processed</th><th>Completion</th></tr></thead>';
            html += '<tbody>';
            
            states.forEach(state => {
                const percentage = ((state.processed_count / state.document_count) * 100).toFixed(2);
                
                html += `<tr>
                    <td>${state.state_name}</td>
                    <td>${state.state_code}</td>
                    <td>${state.document_count}</td>
                    <td>${state.processed_count}</td>
                    <td>
                        <div class="progress">
                            <div class="progress-bar" role="progressbar" style="width: ${percentage}%;"
                                 aria-valuenow="${percentage}" aria-valuemin="0" aria-valuemax="100">
                                ${percentage}%
                            </div>
                        </div>
                    </td>
                </tr>`;
            });
            
            html += '</tbody></table>';
            container.innerHTML = html;
        }
        
        // Initialize dashboard on load
        document.addEventListener('DOMContentLoaded', () => {
            fetchDashboardData();
            
            // Set up refresh button
            document.getElementById('refreshBtn').addEventListener('click', fetchDashboardData);
            
            // Auto-refresh every 30 seconds
            setInterval(fetchDashboardData, 30000);
        });
    </script>
</body>
</html>
""")
        logger.info(f"Created template file at {template_path}")

def generate_state_data():
    """Get data for state statistics"""
    stats = db_manager.get_processing_statistics()
    
    state_stats = stats.get('state_stats', [])
    
    return {
        'states_count': stats.get('states_count', 0),
        'top_states': state_stats
    }

def generate_document_type_data():
    """Get data for document type statistics"""
    stats = db_manager.get_processing_statistics()
    return stats.get('document_types', [])

def generate_processing_history():
    """Generate processing history data"""
    try:
        # Query recent runs
        session = db_manager.get_session()
        from tinycrops_lawyer.database.models import ProcessingRun
        from sqlalchemy import func, cast, Date
        
        # Group by date and sum documents processed
        query = (
            session.query(
                cast(ProcessingRun.end_time, Date).label('date'),
                func.sum(ProcessingRun.documents_processed).label('processed')
            )
            .filter(ProcessingRun.status == 'completed')
            .filter(ProcessingRun.end_time != None)
            .group_by(cast(ProcessingRun.end_time, Date))
            .order_by(cast(ProcessingRun.end_time, Date))
        )
        
        results = query.all()
        session.close()
        
        # Format for chart
        history = [
            {
                'date': result.date.strftime('%Y-%m-%d') if hasattr(result.date, 'strftime') else str(result.date),
                'processed': result.processed
            }
            for result in results
        ]
        
        # Ensure we have at least some data points
        if not history:
            # Add empty data for the last 7 days
            today = datetime.now().date()
            for i in range(7):
                date = today - timedelta(days=i)
                history.append({
                    'date': date.strftime('%Y-%m-%d'),
                    'processed': 0
                })
            
            # Reverse to get chronological order
            history.reverse()
        
        return history
    
    except Exception as e:
        logger.error(f"Error generating processing history: {e}")
        return []

@app.route('/')
def index():
    """Render the dashboard index page"""
    create_template_if_missing()
    return render_template('index.html')

@app.route('/api/dashboard-data')
def dashboard_data():
    """API endpoint to get dashboard data"""
    try:
        stats = db_manager.get_processing_statistics()
        
        # Format the data for the dashboard
        data = {
            'overall_stats': {
                'total_documents': stats.get('total_documents', 0),
                'processed_documents': stats.get('processed_documents', 0),
                'percentage': stats.get('overall_percentage', 0)
            },
            'state_data': generate_state_data(),
            'document_types': generate_document_type_data(),
            'processing_history': generate_processing_history(),
            'recent_runs': stats.get('recent_runs', [])
        }
        
        return jsonify(data)
    
    except Exception as e:
        logger.error(f"Error getting dashboard data: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/run-processor')
def run_processor():
    """Run the processor with default settings"""
    try:
        # Use subprocess to run the processor in the background
        run_script = Path(ROOT_DIR) / 'run_processor.py'
        
        if not run_script.exists():
            return jsonify({'error': 'Processor script not found'}), 404
        
        # Run with process command, limit 10
        cmd = [sys.executable, str(run_script), 'process', '--limit', '10']
        subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        return redirect(url_for('index'))
    
    except Exception as e:
        logger.error(f"Error running processor: {e}")
        return jsonify({'error': str(e)}), 500

def main():
    """Run the dashboard Flask app"""
    create_template_if_missing()
    app.run(debug=True, host='0.0.0.0', port=5000)

if __name__ == '__main__':
    main() 