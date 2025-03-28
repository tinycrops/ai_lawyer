# American Law Dataset Processing

A set of tools for fetching, processing, and analyzing American Law data from the Hugging Face dataset `the-ride-never-ends/american_law`.

## Overview

This project provides scripts to download and process American legal documents from state codes, regulations, ordinances, and more. The processing pipeline extracts structured information from raw HTML data, including:

- Document metadata (state, place name, document type)
- Section extraction from documents
- Automatic document type classification
- Export to structured formats (JSON, CSV)

## Dataset

The dataset contains legal documents from American jurisdictions, including:

- Ordinances
- Codes
- Regulations
- Statutes
- Other legal documents

## Scripts

### 1. `fetch_american_law_data.py`

Downloads and processes data from the dataset, storing it in an SQLite database.

```bash
python fetch_american_law_data.py
```

This script will:
- Download HTML data from the dataset
- Process the HTML to extract text content
- Identify document sections
- Store the processed data in an SQLite database

### 2. `update_metadata.py`

Updates the metadata for documents in the database using the metadata files from the dataset.

```bash
python update_metadata.py
```

This script will:
- Match documents to their metadata files
- Update document information like state, place name
- Determine document types based on content analysis

### 3. `export_processed_data.py`

Exports the processed data from the database to JSON or CSV files.

```bash
python export_processed_data.py
```

This script will:
- Export all documents and sections
- Create separate files for different document types
- Create separate files for different states
- Generate a metadata summary

### 4. `analyze_document_structure.py`

Analyzes the HTML structure of documents to identify patterns for section extraction.

```bash
python analyze_document_structure.py
```

This script will:
- Analyze HTML structure of sample documents
- Identify section patterns
- Save analysis results to JSON files

## Directory Structure

- `data_cache/` - Cached data files from the dataset
- `processed_data/` - Exported processed data files
- `analysis_results/` - Results from document structure analysis
- `american_law_data.db` - SQLite database with processed documents

## Data Schema

### Documents Table

- `doc_id` - Unique document identifier
- `cid` - Content ID from the original dataset
- `place_name` - Name of the place (city, county, etc.)
- `state_code` - Two-letter state code
- `state_name` - Full state name
- `document_type` - Type of document (Ordinance, Code, etc.)
- `content_text` - Extracted text content
- `raw_html` - Original HTML content

### Sections Table

- `section_id` - Unique section identifier
- `doc_id` - Parent document ID
- `section_num` - Section number or identifier
- `section_title` - Section title or heading
- `section_text` - Section text content

## Examples

### Working with the exported JSON data

```python
import json

# Load documents
with open('processed_data/processed_documents.json', 'r') as f:
    documents = [json.loads(line) for line in f]

# Load sections
with open('processed_data/all_sections.json', 'r') as f:
    sections = [json.loads(line) for line in f]

# Print document types
doc_types = set(doc['document_type'] for doc in documents if doc['document_type'])
print(f"Document types: {doc_types}")

# Find documents from North Carolina
nc_docs = [doc for doc in documents if doc['state_code'] == 'NC']
print(f"Found {len(nc_docs)} documents from North Carolina")
```

### Working with the SQLite database

```python
import sqlite3

# Connect to the database
conn = sqlite3.connect('american_law_data.db')
cursor = conn.cursor()

# Query all ordinances
cursor.execute("SELECT * FROM documents WHERE document_type = 'Ordinance'")
ordinances = cursor.fetchall()
print(f"Found {len(ordinances)} ordinances")

# Get sections for a specific document
doc_id = ordinances[0][0]  # First document's ID
cursor.execute("SELECT * FROM sections WHERE doc_id = ?", (doc_id,))
sections = cursor.fetchall()
print(f"Document has {len(sections)} sections")

conn.close()
```

## License

This project processes data from the Hugging Face dataset `the-ride-never-ends/american_law`. Please refer to the original dataset license for usage rights and restrictions 