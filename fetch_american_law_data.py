#!/usr/bin/env python
"""
Script to fetch specific data from the 'the-ride-never-ends/american_law' dataset,
process it, and store it in the required format.

This script builds on the exploratory analysis from explore_dataset.py
"""
import os
import sys
import json
import sqlite3
import re
from pathlib import Path
from typing import Dict, List, Any, Optional
from collections import defaultdict

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from huggingface_hub import hf_hub_download, list_repo_files

# Constants
DB_PATH = "american_law_data.db"
CACHE_DIR = "data_cache"
OUTPUT_DIR = "processed_data"

# Create directories
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)


def setup_database():
    """Create and set up the SQLite database for storing law data"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create documents table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS documents (
        doc_id TEXT PRIMARY KEY,
        cid TEXT,
        place_name TEXT,
        state_code TEXT,
        state_name TEXT,
        chapter TEXT,
        chapter_num TEXT,
        title TEXT,
        title_num TEXT,
        year INTEGER,
        date TEXT,
        enacted TEXT,
        document_type TEXT,
        content_text TEXT,
        raw_html TEXT,
        citation_text TEXT,
        processed INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Create sections table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS sections (
        section_id TEXT PRIMARY KEY,
        doc_id TEXT,
        section_num TEXT,
        section_title TEXT,
        section_text TEXT,
        FOREIGN KEY (doc_id) REFERENCES documents (doc_id)
    )
    ''')
    
    # Create citations table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS citations (
        citation_id TEXT PRIMARY KEY,
        doc_id TEXT,
        citation_text TEXT,
        citation_type TEXT,
        source_id TEXT,
        target_id TEXT,
        FOREIGN KEY (doc_id) REFERENCES documents (doc_id)
    )
    ''')
    
    conn.commit()
    conn.close()


def get_dataset_files(file_type='html', limit=None):
    """
    Get a list of dataset files of the specified type
    
    Args:
        file_type: Type of files to get ('html', 'citation', or 'metadata')
        limit: Maximum number of files to return
        
    Returns:
        List of file paths
    """
    print(f"Getting list of {file_type} files...")
    
    try:
        # Get all files in the dataset repo
        all_files = list_repo_files('the-ride-never-ends/american_law', repo_type='dataset')
        
        # Filter by file type
        if file_type == 'html':
            filtered_files = [f for f in all_files if f.endswith('_html.parquet')]
        elif file_type == 'citation':
            filtered_files = [f for f in all_files if f.endswith('_citation.parquet')]
        elif file_type == 'metadata':
            filtered_files = [f for f in all_files if f.startswith('american_law/metadata/') and f.endswith('.json')]
        else:
            filtered_files = all_files
        
        # Limit number of files if specified
        if limit and limit < len(filtered_files):
            filtered_files = filtered_files[:limit]
        
        print(f"Found {len(filtered_files)} {file_type} files")
        return filtered_files
        
    except Exception as e:
        print(f"Error getting file list: {e}")
        return []


def download_parquet_file(file_path):
    """
    Download a specific parquet file from the dataset
    
    Args:
        file_path: Path to the file in the dataset
        
    Returns:
        DataFrame with the file contents
    """
    cache_path = os.path.join(CACHE_DIR, os.path.basename(file_path))
    
    # Check if already cached
    if os.path.exists(cache_path):
        return pd.read_parquet(cache_path)
    
    try:
        # Download the file
        downloaded_path = hf_hub_download(
            repo_id='the-ride-never-ends/american_law',
            filename=file_path,
            repo_type='dataset'
        )
        
        # Load as DataFrame
        df = pd.read_parquet(downloaded_path)
        
        # Cache for future use
        df.to_parquet(cache_path)
        
        return df
        
    except Exception as e:
        print(f"Error downloading file {file_path}: {e}")
        return pd.DataFrame()


def download_metadata_file(file_path):
    """
    Download a specific metadata JSON file from the dataset
    
    Args:
        file_path: Path to the file in the dataset
        
    Returns:
        Dictionary with the metadata
    """
    cache_path = os.path.join(CACHE_DIR, os.path.basename(file_path))
    
    # Check if already cached
    if os.path.exists(cache_path):
        with open(cache_path, 'r') as f:
            return json.load(f)
    
    try:
        # Download the file
        downloaded_path = hf_hub_download(
            repo_id='the-ride-never-ends/american_law',
            filename=file_path,
            repo_type='dataset'
        )
        
        # Load as JSON
        with open(downloaded_path, 'r') as f:
            metadata = json.load(f)
        
        # Cache for future use
        with open(cache_path, 'w') as f:
            json.dump(metadata, f)
        
        return metadata
        
    except Exception as e:
        print(f"Error downloading metadata file {file_path}: {e}")
        return {}


def extract_text_from_html(html_content):
    """
    Extract readable text from HTML content
    
    Args:
        html_content: Raw HTML string
        
    Returns:
        Cleaned text content
    """
    if not html_content:
        return ""
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.extract()
        
        # Get text
        text = soup.get_text(separator=" ", strip=True)
        
        # Clean up whitespace
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = '\n'.join(chunk for chunk in chunks if chunk)
        
        return text
        
    except Exception as e:
        print(f"Error extracting text from HTML: {e}")
        return ""


def extract_sections_from_html(html_content, doc_id):
    """
    Extract sections from HTML content
    
    Args:
        html_content: Raw HTML string
        doc_id: Document ID
        
    Returns:
        List of section dictionaries
    """
    sections = []
    
    if not html_content:
        return sections
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # First attempt: Look for section elements with explicit classes
        section_elements = soup.find_all(['div', 'section'], 
                                    class_=['section', 'sec', 'statute-section', 'chunk-content'])
        
        if section_elements:
            for i, element in enumerate(section_elements):
                # Create section ID
                section_id = f"{doc_id}_sec_{i+1}"
                
                # Try to find heading
                heading = element.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'strong'])
                heading_text = heading.get_text().strip() if heading else ""
                
                # Get section content
                section_text = element.get_text().strip()
                if heading and heading_text:
                    # Remove heading text from content if possible
                    section_text = section_text.replace(heading_text, "", 1).strip()
                
                sections.append({
                    "section_id": section_id,
                    "doc_id": doc_id,
                    "section_num": str(i+1),
                    "section_title": heading_text,
                    "section_text": section_text
                })
        
        # Second attempt: Try heading-based approach
        if not sections:
            heading_tags = soup.find_all(['h1', 'h2', 'h3', 'h4'])
            
            for i, heading in enumerate(heading_tags):
                # Create section ID
                section_id = f"{doc_id}_sec_{i+1}"
                
                # Get section title
                section_title = heading.get_text(strip=True)
                
                # Extract section number if present
                section_num = str(i+1)
                match = re.search(r'^\s*(?:Section|§)\s*([0-9A-Za-z\.\-]+)', section_title)
                if match:
                    section_num = match.group(1)
                
                # Get section text (all elements until next heading)
                section_text = []
                for element in heading.next_siblings:
                    if element.name in ['h1', 'h2', 'h3', 'h4']:
                        break
                    if hasattr(element, 'get_text'):
                        section_text.append(element.get_text(strip=True))
                
                sections.append({
                    "section_id": section_id,
                    "doc_id": doc_id,
                    "section_num": section_num,
                    "section_title": section_title,
                    "section_text": " ".join(section_text)
                })
        
        # Third attempt: Try paragraphs with structure
        if not sections:
            paras = soup.find_all('p')
            structured_paras = [p for p in paras if p.get('id') or p.get('class')]
            
            if structured_paras:
                for i, para in enumerate(structured_paras):
                    # Create section ID
                    section_id = f"{doc_id}_para_{i+1}"
                    
                    # Get text
                    para_text = para.get_text().strip()
                    
                    # Try to determine if this is a heading
                    is_heading = False
                    if para.find('strong') or para.find('b'):
                        is_heading = True
                    elif len(para_text) < 100 and (para_text.isupper() or para_text.endswith(':')):
                        is_heading = True
                    
                    if is_heading:
                        section_title = para_text
                        section_text = ""
                    else:
                        section_title = f"Paragraph {i+1}"
                        section_text = para_text
                    
                    sections.append({
                        "section_id": section_id,
                        "doc_id": doc_id,
                        "section_num": str(i+1),
                        "section_title": section_title,
                        "section_text": section_text
                    })
        
        # Fourth attempt: Group paragraphs into pseudo-sections
        if not sections:
            paragraphs = soup.find_all('p')
            if paragraphs:
                current_section = []
                all_sections = []
                
                for p in paragraphs:
                    text = p.get_text().strip()
                    if not text:
                        continue
                    
                    # Check if this might be a heading
                    is_heading = len(text) < 100 and (
                        p.find('strong') or 
                        p.find('b') or 
                        p.get('class') or
                        text.isupper() or
                        text.endswith(':')
                    )
                    
                    if is_heading and current_section:
                        # Start a new section
                        all_sections.append(current_section)
                        current_section = [p]
                    else:
                        current_section.append(p)
                
                # Add the last section
                if current_section:
                    all_sections.append(current_section)
                
                # Convert grouped paragraphs to sections
                for i, section_paras in enumerate(all_sections):
                    if not section_paras:
                        continue
                    
                    # First paragraph might be a heading
                    section_title = section_paras[0].get_text().strip() if len(section_paras) > 1 else f"Section {i+1}"
                    
                    # Rest is content
                    content_paras = section_paras[1:] if len(section_paras) > 1 else section_paras
                    section_text = " ".join(p.get_text().strip() for p in content_paras)
                    
                    section_id = f"{doc_id}_group_{i+1}"
                    sections.append({
                        "section_id": section_id,
                        "doc_id": doc_id,
                        "section_num": str(i+1),
                        "section_title": section_title,
                        "section_text": section_text
                    })
                    
        # Last resort: If still no sections, treat the whole document as one section
        if not sections:
            body_text = extract_text_from_html(html_content)
            if body_text:
                sections.append({
                    "section_id": f"{doc_id}_full",
                    "doc_id": doc_id,
                    "section_num": "1",
                    "section_title": "Document",
                    "section_text": body_text
                })
        
        return sections
        
    except Exception as e:
        print(f"Error extracting sections from HTML: {e}")
        return sections


def process_html_file(file_path, process_limit=None):
    """
    Process a single HTML parquet file and store documents in database
    
    Args:
        file_path: Path to the HTML parquet file
        process_limit: Maximum number of documents to process
        
    Returns:
        Dictionary with processing statistics
    """
    print(f"Processing file: {os.path.basename(file_path)}")
    
    stats = {
        "processed": 0,
        "sections_extracted": 0,
        "errors": 0
    }
    
    # Download and read the file
    df = download_parquet_file(file_path)
    
    if df.empty:
        print(f"Failed to load file: {file_path}")
        return stats
    
    # Setup database connection
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Process each row
    for i, row in tqdm(df.iterrows(), total=min(len(df), process_limit or len(df)), 
                      desc="Processing documents"):
        if process_limit and i >= process_limit:
            break
            
        try:
            # Generate document ID
            cid = row.get('cid', '')
            if not cid:
                continue
                
            doc_id = cid
            
            # Check if document already exists
            cursor.execute("SELECT doc_id FROM documents WHERE doc_id = ?", (doc_id,))
            if cursor.fetchone():
                continue
            
            # Extract text from HTML
            html_content = row.get('html', '')
            content_text = ""
            sections = []
            
            if html_content:
                content_text = extract_text_from_html(html_content)
                sections = extract_sections_from_html(html_content, doc_id)
            
            # Insert document into database
            cursor.execute('''
            INSERT INTO documents (
                doc_id, cid, raw_html, content_text, processed
            ) VALUES (?, ?, ?, ?, ?)
            ''', (
                doc_id, 
                cid,
                html_content,
                content_text,
                1
            ))
            
            # Insert sections
            for section in sections:
                cursor.execute('''
                INSERT INTO sections (
                    section_id, doc_id, section_num, section_title, section_text
                ) VALUES (?, ?, ?, ?, ?)
                ''', (
                    section['section_id'],
                    section['doc_id'],
                    section['section_num'],
                    section['section_title'],
                    section['section_text']
                ))
                stats["sections_extracted"] += 1
            
            stats["processed"] += 1
            
            # Commit every 100 documents
            if stats["processed"] % 100 == 0:
                conn.commit()
                
        except Exception as e:
            print(f"Error processing document at row {i}: {e}")
            stats["errors"] += 1
    
    # Final commit
    conn.commit()
    conn.close()
    
    return stats


def get_state_name(state_code):
    """
    Get the full state name from a state code
    
    Args:
        state_code: Two-letter state code
        
    Returns:
        Full state name
    """
    state_map = {
        'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
        'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
        'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
        'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
        'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
        'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
        'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
        'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
        'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
        'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
        'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
        'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
        'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'District of Columbia'
    }
    
    return state_map.get(state_code.upper(), '')


def process_metadata_files(metadata_files):
    """
    Process metadata files and update document records
    
    Args:
        metadata_files: List of metadata JSON files
        
    Returns:
        Number of documents updated
    """
    print("Processing metadata files...")
    
    # Setup database connection
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    updated_count = 0
    
    for file_path in tqdm(metadata_files, desc="Processing metadata files"):
        try:
            # Extract file number from path (e.g., 1008538 from american_law/metadata/1008538.json)
            file_number = os.path.basename(file_path).split('.')[0]
            
            # Download and parse metadata
            metadata = download_metadata_file(file_path)
            
            if not metadata:
                continue
            
            # Get place_name and state_code from metadata
            place_name = metadata.get('place_name', '')
            state_code = metadata.get('state_code', '')
            state_name = get_state_name(state_code)
            
            # Find documents that belong to this file
            # This assumes the CID pattern or that we can match documents by the file number
            # We'll update all documents from this file with the metadata
            cursor.execute("""
            UPDATE documents 
            SET 
                place_name = ?,
                state_code = ?,
                state_name = ?
            WHERE doc_id IN (
                SELECT doc_id FROM documents WHERE doc_id LIKE ?
            )
            """, (
                place_name,
                state_code,
                state_name,
                f"%{file_number}%"  # Match documents with file number in their ID
            ))
            
            updated = cursor.rowcount
            if updated > 0:
                updated_count += updated
                print(f"Updated {updated} documents with metadata from {os.path.basename(file_path)}")
                
        except Exception as e:
            print(f"Error processing metadata file {file_path}: {e}")
    
    # Commit changes
    conn.commit()
    conn.close()
    
    return updated_count


def export_processed_data(limit=None, format='json'):
    """
    Export processed data to files
    
    Args:
        limit: Maximum number of documents to export
        format: Export format ('json' or 'csv')
        
    Returns:
        Number of documents exported
    """
    conn = sqlite3.connect(DB_PATH)
    
    # Query documents
    query = "SELECT * FROM documents WHERE processed = 1"
    if limit:
        query += f" LIMIT {limit}"
    
    docs_df = pd.read_sql_query(query, conn)
    
    # Export documents
    if format == 'json':
        output_file = os.path.join(OUTPUT_DIR, "processed_documents.json")
        docs_df.to_json(output_file, orient='records', lines=True)
    else:
        output_file = os.path.join(OUTPUT_DIR, "processed_documents.csv")
        docs_df.to_csv(output_file, index=False)
    
    print(f"Exported {len(docs_df)} documents to {output_file}")
    
    # Export sections for each document
    sections_exported = 0
    for doc_id in tqdm(docs_df['doc_id'], desc="Exporting sections"):
        sections_df = pd.read_sql_query(
            "SELECT * FROM sections WHERE doc_id = ?", 
            conn, 
            params=(doc_id,)
        )
        
        if len(sections_df) > 0:
            if format == 'json':
                output_file = os.path.join(OUTPUT_DIR, f"sections_{doc_id}.json")
                sections_df.to_json(output_file, orient='records')
            else:
                output_file = os.path.join(OUTPUT_DIR, f"sections_{doc_id}.csv")
                sections_df.to_csv(output_file, index=False)
            sections_exported += len(sections_df)
    
    print(f"Exported {sections_exported} sections")
    
    conn.close()
    return len(docs_df)


def main():
    """Main function to fetch and process the dataset"""
    print("Setting up database...")
    setup_database()
    
    # Ask user for file limit
    try:
        file_limit = int(input("Enter number of HTML files to process (default: 3): ") or "3")
    except ValueError:
        file_limit = 3
    
    # Ask user for document limit per file
    try:
        doc_limit = int(input("Enter number of documents to process per file (default: 50): ") or "50")
    except ValueError:
        doc_limit = 50
    
    # Get HTML files
    html_files = get_dataset_files(file_type='html', limit=file_limit)
    
    if not html_files:
        print("No HTML files found. Exiting.")
        return
    
    # Process HTML files
    total_stats = {
        "processed": 0,
        "sections_extracted": 0,
        "errors": 0
    }
    
    for html_file in html_files:
        file_stats = process_html_file(html_file, process_limit=doc_limit)
        total_stats["processed"] += file_stats["processed"]
        total_stats["sections_extracted"] += file_stats["sections_extracted"]
        total_stats["errors"] += file_stats["errors"]
    
    print(f"\nProcessed {total_stats['processed']} documents")
    print(f"Extracted {total_stats['sections_extracted']} sections")
    print(f"Encountered {total_stats['errors']} errors")
    
    # Get and process metadata
    if total_stats['processed'] > 0:
        metadata_files = get_dataset_files(file_type='metadata', limit=5)
        if metadata_files:
            updated_count = process_metadata_files(metadata_files)
            print(f"Updated metadata for {updated_count} documents")
    
    # Export data
    if total_stats['processed'] > 0:
        print("\nExporting processed data...")
        export_format = input("Export format (json/csv, default: json): ").lower() or "json"
        if export_format not in ['json', 'csv']:
            export_format = 'json'
        
        export_processed_data(format=export_format)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc() 