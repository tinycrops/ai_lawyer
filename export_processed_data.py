#!/usr/bin/env python
"""
Script to export processed American Law data with updated metadata
"""
import os
import sys
import json
import sqlite3
import pandas as pd
from pathlib import Path
from tqdm import tqdm

# Constants
DB_PATH = "american_law_data.db"
OUTPUT_DIR = "processed_data"

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def export_documents(format='json'):
    """
    Export processed documents to files
    
    Args:
        format: Export format ('json' or 'csv')
        
    Returns:
        Number of documents exported
    """
    print(f"Exporting documents in {format} format...")
    conn = sqlite3.connect(DB_PATH)
    
    # Query all documents
    query = "SELECT * FROM documents"
    docs_df = pd.read_sql_query(query, conn)
    
    # Export documents
    if format == 'json':
        output_file = os.path.join(OUTPUT_DIR, "processed_documents.json")
        docs_df.to_json(output_file, orient='records', lines=True)
    else:
        output_file = os.path.join(OUTPUT_DIR, "processed_documents.csv")
        docs_df.to_csv(output_file, index=False)
    
    print(f"Exported {len(docs_df)} documents to {output_file}")
    
    conn.close()
    return len(docs_df)

def export_sections(format='json'):
    """
    Export all sections to files
    
    Args:
        format: Export format ('json' or 'csv')
        
    Returns:
        Number of sections exported
    """
    print(f"Exporting sections in {format} format...")
    conn = sqlite3.connect(DB_PATH)
    
    # Query all sections
    query = "SELECT * FROM sections"
    sections_df = pd.read_sql_query(query, conn)
    
    # Export all sections to one file
    if format == 'json':
        output_file = os.path.join(OUTPUT_DIR, "all_sections.json")
        sections_df.to_json(output_file, orient='records', lines=True)
    else:
        output_file = os.path.join(OUTPUT_DIR, "all_sections.csv")
        sections_df.to_csv(output_file, index=False)
    
    print(f"Exported {len(sections_df)} sections to {output_file}")
    
    # Also export sections by document
    section_count = 0
    unique_docs = sections_df['doc_id'].unique()
    
    for doc_id in tqdm(unique_docs, desc="Exporting sections by document"):
        doc_sections = sections_df[sections_df['doc_id'] == doc_id]
        
        if not doc_sections.empty:
            if format == 'json':
                output_file = os.path.join(OUTPUT_DIR, f"sections_{doc_id}.json")
                doc_sections.to_json(output_file, orient='records')
            else:
                output_file = os.path.join(OUTPUT_DIR, f"sections_{doc_id}.csv")
                doc_sections.to_csv(output_file, index=False)
            
            section_count += len(doc_sections)
    
    conn.close()
    return section_count

def export_by_state(format='json'):
    """
    Export documents grouped by state
    
    Args:
        format: Export format ('json' or 'csv')
        
    Returns:
        Number of states exported
    """
    print("Exporting documents grouped by state...")
    
    conn = sqlite3.connect(DB_PATH)
    
    # Get list of states with documents
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT state_code FROM documents WHERE state_code IS NOT NULL")
    states = [row[0] for row in cursor.fetchall()]
    
    for state_code in states:
        # Get documents for this state
        query = "SELECT * FROM documents WHERE state_code = ?"
        docs_df = pd.read_sql_query(query, conn, params=(state_code,))
        
        if not docs_df.empty:
            if format == 'json':
                output_file = os.path.join(OUTPUT_DIR, f"state_{state_code}.json")
                docs_df.to_json(output_file, orient='records', lines=True)
            else:
                output_file = os.path.join(OUTPUT_DIR, f"state_{state_code}.csv")
                docs_df.to_csv(output_file, index=False)
            
            print(f"Exported {len(docs_df)} documents for state {state_code}")
    
    conn.close()
    return len(states)

def export_by_document_type(format='json'):
    """
    Export documents grouped by document type
    
    Args:
        format: Export format ('json' or 'csv')
        
    Returns:
        Number of document types exported
    """
    print("Exporting documents grouped by document type...")
    
    conn = sqlite3.connect(DB_PATH)
    
    # Get list of document types
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT document_type FROM documents WHERE document_type IS NOT NULL")
    doc_types = [row[0] for row in cursor.fetchall()]
    
    for doc_type in doc_types:
        # Get documents of this type
        query = "SELECT * FROM documents WHERE document_type = ?"
        docs_df = pd.read_sql_query(query, conn, params=(doc_type,))
        
        if not docs_df.empty:
            # Sanitize document type for filename
            safe_type = doc_type.lower().replace(' ', '_')
            
            if format == 'json':
                output_file = os.path.join(OUTPUT_DIR, f"type_{safe_type}.json")
                docs_df.to_json(output_file, orient='records', lines=True)
            else:
                output_file = os.path.join(OUTPUT_DIR, f"type_{safe_type}.csv")
                docs_df.to_csv(output_file, index=False)
            
            print(f"Exported {len(docs_df)} documents of type {doc_type}")
    
    conn.close()
    return len(doc_types)

def export_metadata_summary(format='json'):
    """
    Export a summary of document metadata
    
    Args:
        format: Export format ('json' or 'csv')
    """
    print("Exporting metadata summary...")
    
    conn = sqlite3.connect(DB_PATH)
    
    # Create a summary dataframe with key metadata
    query = """
    SELECT 
        doc_id, cid, place_name, state_code, state_name, 
        document_type, 
        (SELECT COUNT(*) FROM sections WHERE sections.doc_id = documents.doc_id) as section_count
    FROM documents
    """
    summary_df = pd.read_sql_query(query, conn)
    
    # Export summary
    if format == 'json':
        output_file = os.path.join(OUTPUT_DIR, "metadata_summary.json")
        summary_df.to_json(output_file, orient='records', lines=True)
    else:
        output_file = os.path.join(OUTPUT_DIR, "metadata_summary.csv")
        summary_df.to_csv(output_file, index=False)
    
    print(f"Exported metadata summary for {len(summary_df)} documents")
    
    conn.close()

def main():
    print("Exporting processed American Law data...")
    
    # Make sure the database exists
    if not os.path.exists(DB_PATH):
        print(f"Error: Database file {DB_PATH} not found. Run fetch_american_law_data.py first.")
        return
    
    # Ask user for export format
    format_input = input("Export format (json/csv, default: json): ").lower() or "json"
    export_format = format_input if format_input in ['json', 'csv'] else 'json'
    
    # Export data
    export_documents(format=export_format)
    export_sections(format=export_format)
    export_by_state(format=export_format)
    export_by_document_type(format=export_format)
    export_metadata_summary(format=export_format)
    
    print("\nExport complete. Check the processed_data directory for all exported files.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc() 