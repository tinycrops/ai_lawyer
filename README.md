# American Law Dataset Processor

This project processes the "the-ride-never-ends/american_law" dataset from Hugging Face, which contains a vast collection of American legal documents. The system normalizes various HTML/XML schemas using an LLM-powered adapter layer and tracks the processing status of each document.

## Project Purpose

The primary goals of this project are:

1. Explore and analyze the dataset structure
2. Identify common HTML/XML schemas in the dataset
3. Create an adapter layer using an LLM (Gemini 2.0 Flash) to normalize the various schemas
4. Track processing status of documents (loaded, processed, translated)
5. Generate standardized, structured data from the legal documents

## System Architecture

The system consists of several components:

1. **Data Loader**: Fetches and caches files from the Hugging Face dataset
2. **Schema Analyzer**: Identifies patterns in HTML structures to categorize documents
3. **Document Processor**: Extracts and processes individual documents
4. **LLM Adapter**: Uses Google's Gemini to normalize varied document formats
5. **Tracking Database**: SQLite database that monitors processing status

### Database Schema

The system uses a SQLite database with the following tables:

- **files**: Tracks all files in the dataset
- **schemas**: Stores identified HTML schema patterns
- **documents**: Tracks individual documents and their processing status
- **citations**: Links citations to their corresponding documents

## How to Run the Code

### Prerequisites

- Python 3.8+
- Hugging Face account
- Google API key for Gemini

### Installation

1. Clone the repository
2. Install required packages:

```bash
pip install pandas pyarrow beautifulsoup4 huggingface_hub google-generativeai tqdm
```

3. Login to Hugging Face:

```bash
huggingface-cli login
```

4. Set up your Google API key:

```bash
export GOOGLE_API_KEY='your_api_key'
```

### Running the System

To run the entire processing pipeline:

```bash
python american_law_processor.py
```

To explore the dataset structure first:

```bash
python analyze_american_law.py
```

## Processing and Normalizing Schemas

The system handles the varied HTML/XML schemas in the dataset through multiple steps:

1. **Schema Identification**: 
   - Extract HTML structure patterns
   - Create hash signatures for each unique pattern
   - Map schema patterns to document types

2. **Document Type Detection**:
   - Analyze content for keywords (ordinance, charter, code, etc.)
   - Examine HTML structure (tables, footnotes, etc.)
   - Categorize documents accordingly

3. **LLM-Powered Normalization**:
   - Send document HTML to Gemini model
   - Use schema-aware prompts for targeted extraction
   - Transform raw HTML into structured JSON

4. **Standardization**:
   - Remove HTML formatting and extraneous elements
   - Extract key legal information
   - Maintain cross-references between documents

## Document Processing Tracking

The system uses a comprehensive tracking mechanism:

1. **Processing States**:
   - `is_loaded`: Document has been downloaded and stored
   - `is_processed`: Document has been analyzed and prepared
   - `is_translated`: Document has been normalized by LLM

2. **Progress Monitoring**:
   - Track file counts by type (HTML, citation, metadata)
   - Monitor document processing per schema type
   - Generate statistics on processing status

3. **Error Handling**:
   - Detect and report processing issues
   - Cache successfully processed documents
   - Allow for batch processing to manage rate limits

## Output Files

The system generates the following outputs:

1. `processed/`: Directory containing processed HTML files
2. `american_law_processing.db`: SQLite database with processing status
3. `schema_stats.json`: JSON file with schema statistics

## Future Improvements

- Implement more sophisticated schema detection
- Add support for parallel processing
- Create a visualization dashboard for processing progress
- Implement document relationship mapping
- Add data validation for LLM outputs 