import os
import json
import hashlib
import pandas as pd
import pyarrow.parquet as pq
from bs4 import BeautifulSoup
from huggingface_hub import hf_hub_download, list_repo_files
from collections import defaultdict, Counter
import random
import time
import sqlite3
import google.generativeai as genai
from tqdm import tqdm

# Constants
DB_PATH = "american_law_processing.db"
CACHE_DIR = "cache"
PROCESSED_DIR = "processed"
SAMPLE_SIZE = 50  # For schema identification
LLM_MODEL = "gemini-2.0-flash"  # Gemini model to use

# Create directories
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

# Set up database connection
def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def initialize_database():
    """Create database tables if they don't exist"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Table to track all files in the dataset
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS files (
        file_id TEXT PRIMARY KEY,
        file_path TEXT,
        file_type TEXT,
        document_count INTEGER,
        downloaded INTEGER DEFAULT 0,
        processed INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Table to track document schemas
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS schemas (
        schema_id TEXT PRIMARY KEY,
        schema_hash TEXT,
        document_type TEXT,
        sample_file TEXT,
        sample_html TEXT,
        document_count INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Table to track individual documents
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS documents (
        doc_id TEXT PRIMARY KEY,
        cid TEXT,
        file_id TEXT,
        schema_id TEXT,
        document_type TEXT,
        is_loaded INTEGER DEFAULT 0,
        is_processed INTEGER DEFAULT 0,
        is_translated INTEGER DEFAULT 0,
        translated_text TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (file_id) REFERENCES files (file_id),
        FOREIGN KEY (schema_id) REFERENCES schemas (schema_id)
    )
    ''')
    
    # Table for citation data
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS citations (
        citation_id TEXT PRIMARY KEY,
        cid TEXT,
        doc_id TEXT,
        file_id TEXT,
        citation_text TEXT,
        citation_fields TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (doc_id) REFERENCES documents (doc_id),
        FOREIGN KEY (file_id) REFERENCES files (file_id)
    )
    ''')
    
    conn.commit()
    conn.close()

def get_file_list():
    """Get all files in the dataset repository"""
    files = list_repo_files('the-ride-never-ends/american_law', repo_type='dataset')
    return files

def get_html_files(files):
    """Filter out only the HTML parquet files"""
    return [f for f in files if f.endswith('_html.parquet')]

def get_citation_files(files):
    """Filter out only the citation parquet files"""
    return [f for f in files if f.endswith('_citation.parquet')]

def get_metadata_files(files):
    """Filter out only the metadata JSON files"""
    return [f for f in files if f.startswith('american_law/metadata/') and f.endswith('.json')]

def download_and_read_parquet(filename):
    """Download and read a parquet file"""
    cache_path = f"{CACHE_DIR}/{os.path.basename(filename)}"
    
    if os.path.exists(cache_path):
        return pd.read_parquet(cache_path)
    
    file_path = hf_hub_download(
        repo_id='the-ride-never-ends/american_law',
        filename=filename,
        repo_type='dataset'
    )
    df = pd.read_parquet(file_path)
    
    # Cache the dataframe
    df.to_parquet(cache_path)
    
    return df

def extract_html_structure(html_content):
    """Extract structure from HTML content and return a signature"""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Create a structure representation with tag hierarchy
    structure = []
    tag_hierarchy = []
    
    for tag in soup.find_all(True):
        # Get tag name
        tag_name = tag.name
        
        # Get classes
        classes = tag.get('class', [])
        class_str = '.'.join(sorted(classes)) if classes else ''
        
        # Combine tag and class info
        tag_info = f"{tag_name}[{class_str}]"
        structure.append(tag_info)
        
        # Track parent-child relationships for a hierarchy
        if tag.parent and tag.parent.name:
            parent_classes = tag.parent.get('class', [])
            parent_class_str = '.'.join(sorted(parent_classes)) if parent_classes else ''
            parent_info = f"{tag.parent.name}[{parent_class_str}]"
            hierarchy_info = f"{parent_info} > {tag_info}"
            tag_hierarchy.append(hierarchy_info)
    
    # Create a signature based on unique tag structures and their counts
    counter = Counter(structure)
    signature_parts = [f"{tag}:{count}" for tag, count in sorted(counter.items())]
    signature = ";".join(signature_parts)
    
    # Create a hash of the signature for easier comparison
    signature_hash = hashlib.md5(signature.encode()).hexdigest()
    
    return {
        'signature_hash': signature_hash,
        'tag_count': len(structure),
        'unique_tag_count': len(counter),
        'hierarchy_patterns': Counter(tag_hierarchy).most_common(10)
    }

def extract_document_type_from_html(html_content):
    """Try to extract document type from HTML content"""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Possible indicators of document type
    doc_type = None
    
    # Look for title elements
    title_elements = soup.find_all(['div', 'p'], class_=['chunk-title', 'bc', 'h0'])
    
    for element in title_elements:
        text = element.get_text().strip().lower()
        
        # Check for common document types in the title
        if any(doc_type in text for doc_type in ['ordinance', 'charter', 'code', 'statute', 'regulation', 'footnote']):
            for dtype in ['ordinance', 'charter', 'code', 'statute', 'regulation', 'footnote']:
                if dtype in text:
                    doc_type = dtype.title()
                    break
            break
    
    # Check for tables which might indicate reference material
    if not doc_type and soup.find('table'):
        table_headers = [th.get_text().strip().lower() for th in soup.find_all('th')]
        if table_headers:
            doc_type = 'Reference Table'
    
    # Check for footnotes
    if not doc_type and soup.find(class_='footnote-content'):
        doc_type = 'Footnote'
    
    # Default type if none detected
    if not doc_type:
        doc_type = 'Unknown'
    
    return doc_type

def identify_schemas(html_files, sample_size=SAMPLE_SIZE):
    """Identify HTML schema patterns from a sample of files"""
    # Select a sample of HTML files
    sample_files = random.sample(html_files, min(sample_size, len(html_files)))
    
    schemas = defaultdict(list)
    doc_type_mapping = defaultdict(set)
    
    for file in tqdm(sample_files, desc="Identifying schemas"):
        df = download_and_read_parquet(file)
        
        # Analyze each HTML content in the file
        for i, row in df.iterrows():
            if i >= 5:  # Limit to 5 rows per file for initial analysis
                break
                
            html_content = row['html']
            structure_info = extract_html_structure(html_content)
            
            # Try to identify document type
            doc_type = extract_document_type_from_html(html_content)
            
            # Map schema to document type
            doc_type_mapping[structure_info['signature_hash']].add(doc_type)
            
            # Store file and row info with the schema signature
            schemas[structure_info['signature_hash']].append({
                'file': file,
                'row_id': i,
                'cid': row['cid'],
                'doc_type': doc_type,
                'html': html_content[:1000]  # Store a preview of the HTML
            })
    
    # Store schemas in the database
    conn = get_db_connection()
    cursor = conn.cursor()
    
    for schema_hash, instances in schemas.items():
        doc_types = list(doc_type_mapping[schema_hash])
        primary_doc_type = max(set(doc_types), key=doc_types.count) if doc_types else "Unknown"
        
        # Generate a unique ID for the schema
        schema_id = f"schema_{schema_hash[:8]}"
        
        cursor.execute(
            '''
            INSERT OR REPLACE INTO schemas 
            (schema_id, schema_hash, document_type, sample_file, sample_html, document_count)
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (
                schema_id, 
                schema_hash, 
                primary_doc_type, 
                instances[0]['file'] if instances else None,
                instances[0]['html'] if instances else None,
                len(instances)
            )
        )
    
    conn.commit()
    conn.close()
    
    print(f"Identified {len(schemas)} different HTML schema patterns")
    return schemas

def register_files_in_database(files):
    """Register all dataset files in the database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    html_files = get_html_files(files)
    citation_files = get_citation_files(files)
    metadata_files = get_metadata_files(files)
    
    # Register HTML files
    for file in tqdm(html_files, desc="Registering HTML files"):
        file_id = hashlib.md5(file.encode()).hexdigest()
        
        # Try to get document count (if file is already downloaded)
        doc_count = 0
        cache_path = f"{CACHE_DIR}/{os.path.basename(file)}"
        if os.path.exists(cache_path):
            try:
                df = pd.read_parquet(cache_path)
                doc_count = len(df)
            except:
                pass
        
        cursor.execute(
            '''
            INSERT OR IGNORE INTO files 
            (file_id, file_path, file_type, document_count)
            VALUES (?, ?, ?, ?)
            ''',
            (file_id, file, 'html', doc_count)
        )
    
    # Register citation files
    for file in tqdm(citation_files, desc="Registering citation files"):
        file_id = hashlib.md5(file.encode()).hexdigest()
        
        # Try to get document count (if file is already downloaded)
        doc_count = 0
        cache_path = f"{CACHE_DIR}/{os.path.basename(file)}"
        if os.path.exists(cache_path):
            try:
                df = pd.read_parquet(cache_path)
                doc_count = len(df)
            except:
                pass
        
        cursor.execute(
            '''
            INSERT OR IGNORE INTO files 
            (file_id, file_path, file_type, document_count)
            VALUES (?, ?, ?, ?)
            ''',
            (file_id, file, 'citation', doc_count)
        )
    
    # Register metadata files
    for file in tqdm(metadata_files, desc="Registering metadata files"):
        file_id = hashlib.md5(file.encode()).hexdigest()
        
        cursor.execute(
            '''
            INSERT OR IGNORE INTO files 
            (file_id, file_path, file_type, document_count)
            VALUES (?, ?, ?, ?)
            ''',
            (file_id, file, 'metadata', 1)  # Each metadata file represents one document
        )
    
    conn.commit()
    conn.close()
    
    print(f"Registered {len(html_files)} HTML files, {len(citation_files)} citation files, and {len(metadata_files)} metadata files")

def process_html_file(file_path):
    """Process an HTML file and register its documents"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    file_id = hashlib.md5(file_path.encode()).hexdigest()
    df = download_and_read_parquet(file_path)
    
    # Update file record with document count
    cursor.execute(
        '''
        UPDATE files 
        SET document_count = ?, downloaded = 1
        WHERE file_id = ?
        ''',
        (len(df), file_id)
    )
    
    # Get schema information
    cursor.execute("SELECT schema_id, schema_hash FROM schemas")
    schemas = {row['schema_hash']: row['schema_id'] for row in cursor.fetchall()}
    
    # Process each document in the file
    for i, row in tqdm(df.iterrows(), total=len(df), desc=f"Processing {os.path.basename(file_path)}"):
        cid = row['cid']
        html_content = row['html']
        
        # Extract schema hash and document type
        structure_info = extract_html_structure(html_content)
        schema_hash = structure_info['signature_hash']
        doc_type = extract_document_type_from_html(html_content)
        
        # Get or create schema_id
        if schema_hash not in schemas:
            schema_id = f"schema_{schema_hash[:8]}"
            cursor.execute(
                '''
                INSERT OR IGNORE INTO schemas 
                (schema_id, schema_hash, document_type, sample_file, sample_html, document_count)
                VALUES (?, ?, ?, ?, ?, ?)
                ''',
                (schema_id, schema_hash, doc_type, file_path, html_content[:1000], 1)
            )
            schemas[schema_hash] = schema_id
        else:
            schema_id = schemas[schema_hash]
            cursor.execute(
                "UPDATE schemas SET document_count = document_count + 1 WHERE schema_id = ?",
                (schema_id,)
            )
        
        # Generate a unique document ID
        doc_id = f"doc_{hashlib.md5((file_path + str(i)).encode()).hexdigest()[:12]}"
        
        # Register the document
        cursor.execute(
            '''
            INSERT OR IGNORE INTO documents 
            (doc_id, cid, file_id, schema_id, document_type, is_loaded)
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (doc_id, cid, file_id, schema_id, doc_type, 1)
        )
        
        # Save document to processed directory
        output_path = f"{PROCESSED_DIR}/{doc_id}.html"
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
    
    # Mark file as processed
    cursor.execute("UPDATE files SET processed = 1 WHERE file_id = ?", (file_id,))
    
    conn.commit()
    conn.close()
    
    return len(df)

def process_citation_file(file_path):
    """Process a citation file and link to documents"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    file_id = hashlib.md5(file_path.encode()).hexdigest()
    df = download_and_read_parquet(file_path)
    
    # Update file record with document count
    cursor.execute(
        '''
        UPDATE files 
        SET document_count = ?, downloaded = 1
        WHERE file_id = ?
        ''',
        (len(df), file_id)
    )
    
    # Process each citation in the file
    for i, row in tqdm(df.iterrows(), total=len(df), desc=f"Processing {os.path.basename(file_path)}"):
        cid = row['cid']
        
        # Find the corresponding document
        cursor.execute("SELECT doc_id FROM documents WHERE cid = ?", (cid,))
        result = cursor.fetchone()
        
        if result:
            doc_id = result['doc_id']
            
            # Generate a unique citation ID
            citation_id = f"cit_{hashlib.md5((file_path + str(i)).encode()).hexdigest()[:12]}"
            
            # Store citation fields as JSON
            citation_fields = json.dumps(row.to_dict())
            
            # Create citation record
            cursor.execute(
                '''
                INSERT OR IGNORE INTO citations 
                (citation_id, cid, doc_id, file_id, citation_text, citation_fields)
                VALUES (?, ?, ?, ?, ?, ?)
                ''',
                (citation_id, cid, doc_id, file_id, row.get('bluebook_citation', ''), citation_fields)
            )
    
    # Mark file as processed
    cursor.execute("UPDATE files SET processed = 1 WHERE file_id = ?", (file_id,))
    
    conn.commit()
    conn.close()
    
    return len(df)

def setup_gemini():
    """Set up Gemini model for processing"""
    try:
        # Check for API key in environment variable
        api_key = os.environ.get('GOOGLE_API_KEY')
        if not api_key:
            print("Warning: GOOGLE_API_KEY environment variable not found.")
            print("Please set your API key using: export GOOGLE_API_KEY='your_api_key'")
            return None
        
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(LLM_MODEL)
        return model
    except Exception as e:
        print(f"Error setting up Gemini model: {e}")
        return None

def normalize_html_with_llm(html_content, schema_id, doc_type, model):
    """Normalize HTML content using LLM"""
    if not model:
        return None
    
    try:
        prompt = f"""
        Parse the following HTML content from a legal document of type '{doc_type}' (schema ID: {schema_id}).
        Extract key information in a structured format, removing any extraneous HTML or formatting.
        Return a clean, normalized JSON object containing the most relevant legal information.
        
        HTML Content:
        {html_content}
        """
        
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        print(f"Error normalizing HTML with LLM: {e}")
        return None

def process_documents_with_llm(batch_size=10):
    """Process documents with LLM for normalization"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get documents that are loaded but not processed
    cursor.execute(
        """
        SELECT d.doc_id, d.cid, d.schema_id, d.document_type, s.schema_hash
        FROM documents d
        JOIN schemas s ON d.schema_id = s.schema_id
        WHERE d.is_loaded = 1 AND d.is_processed = 0
        LIMIT ?
        """,
        (batch_size,)
    )
    
    documents = cursor.fetchall()
    
    if not documents:
        print("No documents to process.")
        conn.close()
        return 0
    
    # Set up Gemini model
    model = setup_gemini()
    if not model:
        conn.close()
        return 0
    
    processed_count = 0
    
    for doc in tqdm(documents, desc="Processing documents with LLM"):
        doc_id = doc['doc_id']
        doc_path = f"{PROCESSED_DIR}/{doc_id}.html"
        
        # Load HTML content
        if os.path.exists(doc_path):
            with open(doc_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            # Process with LLM
            normalized_text = normalize_html_with_llm(
                html_content, 
                doc['schema_id'], 
                doc['document_type'],
                model
            )
            
            if normalized_text:
                # Update document as processed
                cursor.execute(
                    """
                    UPDATE documents 
                    SET is_processed = 1, is_translated = 1, translated_text = ?
                    WHERE doc_id = ?
                    """,
                    (normalized_text, doc_id)
                )
                
                # Save normalized text to file
                output_path = f"{PROCESSED_DIR}/{doc_id}.json"
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(normalized_text)
                
                processed_count += 1
            
            # Add a small delay to respect API rate limits
            time.sleep(0.5)
    
    conn.commit()
    conn.close()
    
    return processed_count

def get_processing_stats():
    """Get statistics on processing status"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    stats = {}
    
    # Count files by type and status
    cursor.execute("""
    SELECT file_type, 
           COUNT(*) as total,
           SUM(downloaded) as downloaded,
           SUM(processed) as processed
    FROM files
    GROUP BY file_type
    """)
    
    stats['files'] = {row['file_type']: {
        'total': row['total'],
        'downloaded': row['downloaded'],
        'processed': row['processed']
    } for row in cursor.fetchall()}
    
    # Count documents by status
    cursor.execute("""
    SELECT COUNT(*) as total,
           SUM(is_loaded) as loaded,
           SUM(is_processed) as processed,
           SUM(is_translated) as translated
    FROM documents
    """)
    
    row = cursor.fetchone()
    stats['documents'] = {
        'total': row['total'],
        'loaded': row['loaded'],
        'processed': row['processed'],
        'translated': row['translated']
    }
    
    # Count schemas
    cursor.execute("SELECT COUNT(*) as count FROM schemas")
    stats['schemas'] = cursor.fetchone()['count']
    
    # Get document counts by schema
    cursor.execute("""
    SELECT s.schema_id, s.document_type, COUNT(d.doc_id) as doc_count
    FROM schemas s
    LEFT JOIN documents d ON s.schema_id = d.schema_id
    GROUP BY s.schema_id
    """)
    
    stats['schemas_breakdown'] = {row['schema_id']: {
        'document_type': row['document_type'],
        'document_count': row['doc_count']
    } for row in cursor.fetchall()}
    
    conn.close()
    
    return stats

def main():
    # Initialize database
    initialize_database()
    
    print("Retrieving file list...")
    all_files = get_file_list()
    
    # Register files in database
    register_files_in_database(all_files)
    
    # Identify schemas
    html_files = get_html_files(all_files)
    identify_schemas(html_files, sample_size=min(SAMPLE_SIZE, len(html_files)))
    
    # Process a batch of files
    print("\nProcessing a batch of HTML files...")
    files_to_process = min(5, len(html_files))
    for i in range(files_to_process):
        process_html_file(html_files[i])
    
    # Process a batch of citation files
    print("\nProcessing a batch of citation files...")
    citation_files = get_citation_files(all_files)
    files_to_process = min(5, len(citation_files))
    for i in range(files_to_process):
        process_citation_file(citation_files[i])
    
    # Process a batch of documents with LLM
    print("\nProcessing documents with LLM...")
    processed_count = process_documents_with_llm(batch_size=5)
    print(f"Processed {processed_count} documents with LLM")
    
    # Get processing statistics
    stats = get_processing_stats()
    print("\nProcessing Statistics:")
    print(json.dumps(stats, indent=2))

if __name__ == "__main__":
    main() 