#!/usr/bin/env python
"""
Jurisdiction-based processor for the American Law Dataset.
Organizes and processes legal documents by state and municipality.
"""

import os
import json
import sqlite3
import hashlib
from pathlib import Path
from collections import defaultdict
from tqdm import tqdm
import google.generativeai as genai
from huggingface_hub import hf_hub_download
import pandas as pd
from bs4 import BeautifulSoup
import time
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='location_processor.log'
)
logger = logging.getLogger('location_processor')

# Constants
DB_PATH = "american_law_data.db"
OUTPUT_DIR = "processed_documents"
CACHE_DIR = "data_cache"
LLM_MODEL = "gemini-2.0-flash"  # Gemini model to use
BATCH_SIZE = 10

# Create directories
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(os.path.join(OUTPUT_DIR, "states"), exist_ok=True)

class LocationProcessor:
    """Processes documents based on their geographical jurisdiction"""
    
    def __init__(self, model_name=LLM_MODEL, batch_size=BATCH_SIZE):
        """Initialize the processor"""
        self.model_name = model_name
        self.batch_size = batch_size
        self.db_conn = self._get_db_connection()
        self.model = self._setup_gemini()
        
    def _get_db_connection(self):
        """Return a connection to the SQLite database"""
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    
    def _setup_gemini(self):
        """Configure the Gemini API"""
        try:
            # Look for API key in environment variable or .env file
            from dotenv import load_dotenv
            load_dotenv()
            
            api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
            if not api_key:
                logger.warning("No Google/Gemini API key found. LLM processing will not work.")
                return None
            
            genai.configure(api_key=api_key)
            
            # Configure the model
            generation_config = {
                "temperature": 0.2,
                "top_p": 0.95,
                "top_k": 40,
                "max_output_tokens": 8192,
            }
            
            model = genai.GenerativeModel(
                model_name=self.model_name,
                generation_config=generation_config
            )
            
            return model
        except Exception as e:
            logger.error(f"Error setting up Gemini: {e}")
            return None
    
    def download_and_read_parquet(self, filename):
        """Download and read a parquet file from the dataset"""
        cache_path = f"{CACHE_DIR}/{os.path.basename(filename)}"
        
        if os.path.exists(cache_path):
            return pd.read_parquet(cache_path)
        
        try:
            file_path = hf_hub_download(
                repo_id='the-ride-never-ends/american_law',
                filename=filename,
                repo_type='dataset'
            )
            df = pd.read_parquet(file_path)
            
            # Cache the dataframe
            df.to_parquet(cache_path)
            
            return df
        except Exception as e:
            logger.error(f"Error downloading file {filename}: {e}")
            return None
    
    def extract_jurisdiction_info(self, metadata_files):
        """Extract jurisdiction information from metadata files"""
        states = {}
        places = {}
        documents = {}
        
        for file in tqdm(metadata_files, desc="Processing metadata"):
            try:
                # Skip if not a metadata file
                if not file.startswith("american_law/metadata/") or not file.endswith(".json"):
                    continue
                    
                # Download and read the file
                file_path = hf_hub_download(
                    repo_id='the-ride-never-ends/american_law',
                    filename=file,
                    repo_type='dataset'
                )
                
                with open(file_path, 'r') as f:
                    metadata = json.load(f)
                    
                # Process each document in the metadata
                for doc_id, doc_info in metadata.items():
                    # Skip if missing key information
                    if not doc_id:
                        continue
                    
                    # Extract state information
                    state_code = doc_info.get('state_code')
                    state_name = doc_info.get('state_name')
                    place_name = doc_info.get('place_name')
                    
                    # Skip documents without state or place info
                    if not state_code or not place_name:
                        continue
                    
                    # Create state entry
                    if state_code not in states:
                        states[state_code] = {
                            'state_code': state_code,
                            'state_name': state_name,
                            'document_count': 0
                        }
                    
                    # Create place hash (for consistent IDs)
                    place_id = hashlib.md5(f"{place_name}_{state_code}".encode()).hexdigest()
                    
                    # Create place entry
                    if place_id not in places:
                        places[place_id] = {
                            'place_id': place_id,
                            'place_name': place_name,
                            'state_code': state_code,
                            'document_count': 0
                        }
                    
                    # Create document entry
                    documents[doc_id] = {
                        'doc_id': doc_id,
                        'cid': doc_info.get('cid') or doc_id,
                        'place_id': place_id,
                        'state_code': state_code,
                        'document_type': doc_info.get('document_type'),
                        'year': doc_info.get('year'),
                        'date': doc_info.get('date'),
                        'title': doc_info.get('title'),
                        'chapter': doc_info.get('chapter')
                    }
                    
                    # Increment counts
                    states[state_code]['document_count'] += 1
                    places[place_id]['document_count'] += 1
                    
            except Exception as e:
                logger.error(f"Error processing file {file}: {e}")
        
        return states, places, documents
    
    def save_jurisdiction_info(self, states, places, documents):
        """Save jurisdiction information to the database"""
        cursor = self.db_conn.cursor()
        
        # Insert states
        for state_code, state_info in tqdm(states.items(), desc="Saving states"):
            cursor.execute(
                '''
                INSERT OR REPLACE INTO states 
                (state_code, state_name, document_count)
                VALUES (?, ?, ?)
                ''',
                (
                    state_info['state_code'],
                    state_info['state_name'],
                    state_info['document_count']
                )
            )
        
        # Insert places
        for place_id, place_info in tqdm(places.items(), desc="Saving places"):
            cursor.execute(
                '''
                INSERT OR REPLACE INTO places 
                (place_id, place_name, state_code, document_count)
                VALUES (?, ?, ?, ?)
                ''',
                (
                    place_info['place_id'],
                    place_info['place_name'],
                    place_info['state_code'],
                    place_info['document_count']
                )
            )
        
        # Insert documents
        for doc_id, doc_info in tqdm(documents.items(), desc="Saving documents"):
            cursor.execute(
                '''
                INSERT OR REPLACE INTO jurisdiction_documents 
                (doc_id, cid, place_id, state_code, document_type, year, date, title, chapter)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    doc_info['doc_id'],
                    doc_info['cid'],
                    doc_info['place_id'],
                    doc_info['state_code'],
                    doc_info['document_type'],
                    doc_info['year'],
                    doc_info['date'],
                    doc_info['title'],
                    doc_info['chapter']
                )
            )
        
        self.db_conn.commit()
    
    def process_html_with_llm(self, document, html_content):
        """Process HTML content with the LLM to extract structured data"""
        if not self.model:
            logger.warning("No LLM model available for processing.")
            return None
        
        # Create the prompt
        prompt = f"""
    You are a legal document parser specializing in American municipal law.
    
    DOCUMENT INFORMATION:
    - Document ID: {document['doc_id']}
    - Document Type: {document['document_type'] or 'Unknown'}
    - Jurisdiction: {document.get('place_name', 'Unknown')}, {document.get('state_name', 'Unknown')}
    - Year: {document.get('year', 'Unknown')}
    
    HTML CONTENT:
    {html_content[:50000]}  # Truncate if too long
    
    PARSING INSTRUCTIONS:
    1. Extract the main document content, preserving section structure
    2. Identify section headings, numbers, and content
    3. Format date references consistently as YYYY-MM-DD
    4. Extract any legislative history or citation information
    5. Preserve footnotes and references
    
    OUTPUT FORMAT:
    Return a JSON object with the following fields:
    - document_id: Unique identifier from the document info
    - jurisdiction: Place and state information
    - document_type: The type of document
    - sections: Array of section objects containing:
      - section_id: Unique section identifier (use document_id + "_sec_" + index)
      - section_num: The section number
      - section_title: The section heading
      - section_text: The main text content
      - section_refs: Any references to other sections or documents (optional)
    """
        
        try:
            response = self.model.generate_content(prompt)
            response_text = response.text
            
            # Try to extract JSON from the response
            try:
                # Find JSON object in the response if surrounded by backticks
                if "```json" in response_text:
                    json_text = response_text.split("```json")[1].split("```")[0].strip()
                elif "```" in response_text:
                    json_text = response_text.split("```")[1].split("```")[0].strip()
                else:
                    json_text = response_text
                    
                parsed_data = json.loads(json_text)
                return parsed_data
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing JSON response: {e}")
                logger.error(f"Response text: {response_text[:500]}...")
                return None
                
        except Exception as e:
            logger.error(f"Error processing with LLM: {e}")
            return None
    
    def process_documents_by_jurisdiction(self, state_code=None, limit=100, only_new=False):
        """Process documents organized by jurisdiction"""
        stats = {"processed": 0, "errors": 0}
        
        # Get unprocessed documents
        query = "SELECT jd.*, p.place_name, s.state_name FROM jurisdiction_documents jd " + \
                "JOIN places p ON jd.place_id = p.place_id " + \
                "JOIN states s ON jd.state_code = s.state_code " + \
                "WHERE jd.is_processed = 0 "
        
        params = []
        if state_code:
            query += "AND jd.state_code = ? "
            params.append(state_code)
        
        query += "LIMIT ?"
        params.append(limit)
        
        cursor = self.db_conn.cursor()
        documents = cursor.execute(query, params).fetchall()
        
        # Organize documents by state and place
        doc_by_location = defaultdict(lambda: defaultdict(list))
        for doc in documents:
            doc_by_location[doc['state_code']][doc['place_id']].append(dict(doc))
        
        # Process each state
        for state_code, places in doc_by_location.items():
            state_dir = os.path.join(OUTPUT_DIR, "states", state_code)
            os.makedirs(state_dir, exist_ok=True)
            
            # Process each place
            for place_id, docs in places.items():
                place_dir = os.path.join(state_dir, place_id)
                os.makedirs(place_dir, exist_ok=True)
                
                # Organize documents by type
                doc_by_type = defaultdict(list)
                for doc in docs:
                    doc_type = doc['document_type'] or 'unknown'
                    doc_by_type[doc_type].append(doc)
                
                # Process each document type
                for doc_type, type_docs in doc_by_type.items():
                    type_dir = os.path.join(place_dir, doc_type.lower())
                    os.makedirs(type_dir, exist_ok=True)
                    
                    # Process each document in batch
                    for i in range(0, len(type_docs), self.batch_size):
                        batch = type_docs[i:i+self.batch_size]
                        
                        for doc in batch:
                            try:
                                # Check if HTML file exists in cache
                                html_file = os.path.join(CACHE_DIR, f"{doc['doc_id']}.html")
                                if not os.path.exists(html_file):
                                    # Try to find HTML file with matching doc_id or cid
                                    query = "SELECT html_path FROM html_files WHERE doc_id = ? OR doc_id LIKE ?"
                                    result = cursor.execute(query, (doc['doc_id'], f"%{doc['cid']}%")).fetchone()
                                    
                                    if result:
                                        html_file = result['html_path']
                                    else:
                                        # Attempt to download from HuggingFace based on document id
                                        try:
                                            file_path = hf_hub_download(
                                                repo_id='the-ride-never-ends/american_law',
                                                filename=f"american_law/html/{doc['doc_id']}.html",
                                                repo_type='dataset'
                                            )
                                            html_file = file_path
                                        except Exception as e:
                                            logger.error(f"Could not find HTML file for document {doc['doc_id']}: {e}")
                                            stats["errors"] += 1
                                            continue
                                
                                # Read HTML content
                                with open(html_file, 'r', encoding='utf-8') as f:
                                    html_content = f.read()
                                
                                # Process with LLM
                                start_time = time.time()
                                processed_data = self.process_html_with_llm(doc, html_content)
                                processing_time = time.time() - start_time
                                
                                if processed_data:
                                    # Save processed data
                                    output_file = os.path.join(type_dir, f"{doc['doc_id']}.json")
                                    with open(output_file, 'w') as f:
                                        json.dump(processed_data, f, indent=2)
                                    
                                    # Update document status in database
                                    cursor.execute(
                                        "UPDATE jurisdiction_documents SET is_processed = 1 WHERE doc_id = ?",
                                        (doc['doc_id'],)
                                    )
                                    self.db_conn.commit()
                                    
                                    # Update statistics
                                    stats["processed"] += 1
                                    
                                    # Update place processed count
                                    cursor.execute(
                                        "UPDATE places SET processed_count = processed_count + 1 WHERE place_id = ?",
                                        (place_id,)
                                    )
                                    
                                    # Update state processed count
                                    cursor.execute(
                                        "UPDATE states SET processed_count = processed_count + 1 WHERE state_code = ?",
                                        (state_code,)
                                    )
                                    
                                    self.db_conn.commit()
                                else:
                                    stats["errors"] += 1
                            except Exception as e:
                                logger.error(f"Error processing document {doc['doc_id']}: {e}")
                                stats["errors"] += 1
                                continue
        
        return stats
    
    def generate_jurisdiction_reports(self):
        """Generate reports on jurisdiction processing status"""
        cursor = self.db_conn.cursor()
        
        # Get state statistics
        states = cursor.execute(
            """
            SELECT state_code, state_name, document_count, processed_count,
                   (CAST(processed_count AS FLOAT) / CAST(document_count AS FLOAT)) * 100 AS completion_percentage
            FROM states
            ORDER BY document_count DESC
            """
        ).fetchall()
        
        # Get document type statistics
        doc_types = cursor.execute(
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
            places = cursor.execute(
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
        
        # Create report
        report = {
            'states': [dict(state) for state in states],
            'document_types': [dict(doc_type) for doc_type in doc_types],
            'top_places_by_state': top_places
        }
        
        # Save report
        report_file = os.path.join(OUTPUT_DIR, "jurisdiction_report.json")
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        # Print summary
        logger.info("\nJurisdiction Processing Summary:")
        logger.info(f"Total States: {len(states)}")
        logger.info(f"Total Document Types: {len(doc_types)}")
        logger.info("\nTop 5 States by Document Count:")
        for state in states[:5]:
            logger.info(f"  {state['state_name']} ({state['state_code']}): {state['document_count']} documents, {state['completion_percentage']:.1f}% complete")
        
        logger.info("\nDocument Types:")
        for doc_type in doc_types:
            if doc_type['document_type']:
                logger.info(f"  {doc_type['document_type']}: {doc_type['count']} documents, {doc_type['completion_percentage']:.1f}% complete")
        
        return report

def initialize_database():
    """Create database tables for jurisdiction-based processing"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # States table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS states (
        state_code TEXT PRIMARY KEY,
        state_name TEXT,
        document_count INTEGER DEFAULT 0,
        processed_count INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Places (jurisdictions) table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS places (
        place_id TEXT PRIMARY KEY,
        place_name TEXT,
        state_code TEXT,
        document_count INTEGER DEFAULT 0,
        processed_count INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (state_code) REFERENCES states (state_code)
    )
    ''')
    
    # Documents table with jurisdiction info
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS jurisdiction_documents (
        doc_id TEXT PRIMARY KEY,
        cid TEXT,
        place_id TEXT,
        state_code TEXT,
        document_type TEXT,
        year TEXT,
        date TEXT,
        title TEXT,
        chapter TEXT,
        is_processed INTEGER DEFAULT 0,
        html_path TEXT,
        metadata_path TEXT,
        schema_hash TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (place_id) REFERENCES places (place_id),
        FOREIGN KEY (state_code) REFERENCES states (state_code)
    )
    ''')
    
    # HTML files table for tracking source files
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS html_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        doc_id TEXT,
        html_path TEXT,
        file_size INTEGER,
        downloaded_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    conn.commit()
    conn.close()

def main():
    """Main function to run the jurisdiction-based processor"""
    # Initialize database
    print("Initializing database...")
    initialize_database()
    
    # Create processor
    processor = LocationProcessor()
    
    # Check if metadata files need to be processed
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    state_count = cursor.execute("SELECT COUNT(*) FROM states").fetchone()[0]
    conn.close()
    
    if state_count == 0:
        print("Extracting jurisdiction information...")
        
        # Get metadata files
        try:
            from huggingface_hub import list_repo_files
            files = list_repo_files('the-ride-never-ends/american_law', repo_type='dataset')
            metadata_files = [f for f in files if f.startswith("american_law/metadata/") and f.endswith(".json")]
            
            # Extract jurisdiction info
            states, places, documents = processor.extract_jurisdiction_info(metadata_files)
            
            # Save to database
            processor.save_jurisdiction_info(states, places, documents)
            
            print(f"Extracted information for {len(states)} states, {len(places)} places, and {len(documents)} documents")
        except Exception as e:
            print(f"Error extracting jurisdiction info: {e}")
            return
    
    # Process documents by jurisdiction
    print("Processing documents by jurisdiction...")
    stats = processor.process_documents_by_jurisdiction(limit=10)
    
    # Generate reports
    print("Generating jurisdiction reports...")
    processor.generate_jurisdiction_reports()
    
    print(f"Jurisdiction processing completed. Processed {stats['processed']} documents with {stats['errors']} errors.")

if __name__ == "__main__":
    main() 