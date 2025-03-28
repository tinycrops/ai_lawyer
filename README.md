# American Law Dataset Processor

A comprehensive system for processing, parsing, and organizing the American Law Dataset by jurisdiction (state, city, municipality).

## Overview

This project provides a structured approach to process legal documents from the American Law Dataset, organizing them by jurisdiction and document type. It uses the Gemini LLM API to extract structured information from HTML documents.

### Key Components

- **Location Processor**: Organizes documents by geographical jurisdiction, allowing for state-by-state and municipality-level processing
- **Schema Parser**: Identifies document schemas and uses intelligent parsing based on document structure
- **Schema Manager**: Manages document schemas and optimizes parsing strategies
- **Interactive Dashboard**: Real-time monitoring of processing status with visualizations
- **Database Utilities**: Manages the SQLite database that tracks processing status and document metadata

## Getting Started

### Prerequisites

- Python 3.8+
- Gemini API key (set as environment variable `GEMINI_API_KEY`)
- Required Python packages (see `requirements.txt`)

### Installation

1. Clone the repository
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Initialize the system:
   ```
   python run_processor.py init
   ```

### Basic Usage

The system is driven by the `run_processor.py` command-line utility:

```bash
# Process documents by state
python run_processor.py process --state CA --limit 100

# Analyze document schemas
python run_processor.py schema --analyze --limit 200

# Generate progress report
python run_processor.py report --progress

# Show database statistics
python run_processor.py db --stats
```

### Using the Dashboard

The interactive dashboard provides real-time monitoring of the processing status:

1. Start the dashboard:
   ```bash
   ./start_dashboard.sh
   ```
   Or manually:
   ```bash
   python dashboard.py
   ```

2. Access the dashboard in your web browser at http://localhost:5000

3. Dashboard features:
   - Overall processing progress
   - State coverage statistics
   - Document type breakdown
   - Processing history charts
   - Recent processing runs
   - Interactive data refresh
   - Direct processor execution

4. Simulate processing for testing:
   ```bash
   python simulate_processing.py --continuous
   ```

## Directory Structure

```
.
├── american_law_data.db       # SQLite database
├── dashboard.py               # Interactive web dashboard
├── data_cache/                # Downloaded raw data
├── processed_documents/       # Processed and structured data output
├── schema_cache/              # Schema data and patterns
├── reports/                   # Visualization reports
├── templates/                 # Dashboard HTML templates
└── db_backups/                # Database backup files
```

## Processing Workflow

1. **Data Acquisition**: Download and organize raw HTML documents by jurisdiction
2. **Schema Analysis**: Identify document patterns and structure to optimize parsing
3. **Document Processing**: Use the Gemini LLM to extract structured data from documents
4. **Progress Tracking**: Monitor processing status through the interactive dashboard
5. **Data Utilization**: Utilize the processed data for legal research and analysis

## Key Files

- `location_processor.py`: Main processor that organizes documents by jurisdiction
- `schema_parser.py`: Parser that normalizes HTML based on document schema
- `schema_manager.py`: Manager for document schemas and parsing strategies
- `db_utils.py`: Database utility functions
- `dashboard.py`: Interactive web dashboard for monitoring progress
- `simulate_processing.py`: Tool for simulating document processing
- `run_processor.py`: Command-line utility for running the processor

## Database Schema

The system uses an SQLite database with the following tables:

- `states`: Tracks states and their document counts
- `places`: Tracks municipalities and their document counts
- `jurisdiction_documents`: Records document metadata and processing status
- `processing_runs`: Logs processing runs with timing and statistics

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Contact

For questions and support, please open an issue in the repository. 