#!/usr/bin/env python
"""
Database utilities for the American Law Dataset processing.
Provides helper functions for database operations and queries.
"""

import os
import sqlite3
import json
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='db_utils.log'
)
logger = logging.getLogger('db_utils')

# Constants
DB_PATH = "american_law_data.db"
DB_BACKUP_DIR = "db_backups"

# Ensure backup directory exists
os.makedirs(DB_BACKUP_DIR, exist_ok=True)

def get_connection(read_only=False):
    """
    Get a connection to the SQLite database
    
    Args:
        read_only: Whether to open the connection in read-only mode
        
    Returns:
        A SQLite connection object
    """
    if read_only:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    else:
        conn = sqlite3.connect(DB_PATH)
    
    # Enable foreign keys
    conn.execute("PRAGMA foreign_keys = ON")
    
    # Return rows as dictionaries
    conn.row_factory = sqlite3.Row
    
    return conn

def initialize_database():
    """
    Initialize the database with the necessary tables
    
    Returns:
        True if successful, False otherwise
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Create states table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS states (
            state_code TEXT PRIMARY KEY,
            state_name TEXT NOT NULL,
            document_count INTEGER DEFAULT 0,
            processed_count INTEGER DEFAULT 0,
            last_updated TIMESTAMP
        )
        """)
        
        # Create places table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS places (
            place_id TEXT PRIMARY KEY,
            state_code TEXT NOT NULL,
            place_name TEXT NOT NULL,
            place_type TEXT,
            document_count INTEGER DEFAULT 0,
            processed_count INTEGER DEFAULT 0,
            last_updated TIMESTAMP,
            FOREIGN KEY (state_code) REFERENCES states(state_code)
        )
        """)
        
        # Create jurisdiction_documents table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS jurisdiction_documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id TEXT UNIQUE NOT NULL,
            state_code TEXT,
            place_id TEXT,
            document_type TEXT,
            document_date TEXT,
            html_path TEXT,
            metadata_path TEXT,
            schema_hash TEXT,
            is_processed BOOLEAN DEFAULT 0,
            processed_path TEXT,
            processed_date TIMESTAMP,
            processing_time REAL,
            processing_error TEXT,
            FOREIGN KEY (state_code) REFERENCES states(state_code),
            FOREIGN KEY (place_id) REFERENCES places(place_id)
        )
        """)
        
        # Create processing_runs table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS processing_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP,
            status TEXT,
            documents_processed INTEGER DEFAULT 0,
            errors_count INTEGER DEFAULT 0,
            model_name TEXT,
            notes TEXT
        )
        """)
        
        # Create indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_docs_state ON jurisdiction_documents(state_code)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_docs_place ON jurisdiction_documents(place_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_docs_processed ON jurisdiction_documents(is_processed)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_docs_type ON jurisdiction_documents(document_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_docs_schema ON jurisdiction_documents(schema_hash)")
        
        conn.commit()
        conn.close()
        
        logger.info("Database initialized successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        return False

def add_state(state_code, state_name):
    """
    Add a state to the database
    
    Args:
        state_code: Two-letter state code
        state_name: Full state name
        
    Returns:
        True if successful, False otherwise
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """
            INSERT OR IGNORE INTO states (state_code, state_name, last_updated)
            VALUES (?, ?, ?)
            """,
            (state_code, state_name, datetime.now())
        )
        
        conn.commit()
        conn.close()
        
        logger.info(f"Added state: {state_code} - {state_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error adding state {state_code}: {str(e)}")
        return False

def add_place(place_id, state_code, place_name, place_type=None):
    """
    Add a place to the database
    
    Args:
        place_id: Unique identifier for the place
        state_code: Two-letter state code
        place_name: Name of the place
        place_type: Type of place (e.g., city, county)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """
            INSERT OR IGNORE INTO places (place_id, state_code, place_name, place_type, last_updated)
            VALUES (?, ?, ?, ?, ?)
            """,
            (place_id, state_code, place_name, place_type, datetime.now())
        )
        
        conn.commit()
        conn.close()
        
        logger.info(f"Added place: {place_id} - {place_name} ({state_code})")
        return True
        
    except Exception as e:
        logger.error(f"Error adding place {place_id}: {str(e)}")
        return False

def add_document(document_id, state_code, place_id, document_type, document_date, 
                 html_path, metadata_path=None, schema_hash=None):
    """
    Add a document to the database
    
    Args:
        document_id: Unique identifier for the document
        state_code: Two-letter state code
        place_id: Identifier for the place
        document_type: Type of document
        document_date: Date of the document
        html_path: Path to the HTML file
        metadata_path: Path to the metadata file
        schema_hash: Hash of the document schema
        
    Returns:
        True if successful, False otherwise
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """
            INSERT OR IGNORE INTO jurisdiction_documents 
            (document_id, state_code, place_id, document_type, document_date, 
             html_path, metadata_path, schema_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (document_id, state_code, place_id, document_type, document_date, 
             html_path, metadata_path, schema_hash)
        )
        
        # Update document counts for state and place
        if cursor.rowcount > 0:
            cursor.execute(
                """
                UPDATE states 
                SET document_count = document_count + 1,
                    last_updated = ?
                WHERE state_code = ?
                """,
                (datetime.now(), state_code)
            )
            
            cursor.execute(
                """
                UPDATE places 
                SET document_count = document_count + 1,
                    last_updated = ?
                WHERE place_id = ?
                """,
                (datetime.now(), place_id)
            )
        
        conn.commit()
        conn.close()
        
        logger.info(f"Added document: {document_id} ({state_code}, {place_id})")
        return True
        
    except Exception as e:
        logger.error(f"Error adding document {document_id}: {str(e)}")
        return False

def mark_document_processed(document_id, processed_path, processing_time=None, error=None):
    """
    Mark a document as processed
    
    Args:
        document_id: Unique identifier for the document
        processed_path: Path to the processed file
        processing_time: Time taken to process the document in seconds
        error: Error message if processing failed
        
    Returns:
        True if successful, False otherwise
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Get current document info
        cursor.execute(
            "SELECT state_code, place_id FROM jurisdiction_documents WHERE document_id = ?",
            (document_id,)
        )
        doc_info = cursor.fetchone()
        
        if not doc_info:
            logger.warning(f"Document {document_id} not found")
            conn.close()
            return False
        
        # Update document status
        is_processed = 1 if error is None else 0
        cursor.execute(
            """
            UPDATE jurisdiction_documents
            SET is_processed = ?,
                processed_path = ?,
                processed_date = ?,
                processing_time = ?,
                processing_error = ?
            WHERE document_id = ?
            """,
            (is_processed, processed_path, datetime.now(), processing_time, error, document_id)
        )
        
        # Update processed counts for state and place if successful
        if is_processed:
            cursor.execute(
                """
                UPDATE states 
                SET processed_count = processed_count + 1,
                    last_updated = ?
                WHERE state_code = ?
                """,
                (datetime.now(), doc_info['state_code'])
            )
            
            cursor.execute(
                """
                UPDATE places 
                SET processed_count = processed_count + 1,
                    last_updated = ?
                WHERE place_id = ?
                """,
                (datetime.now(), doc_info['place_id'])
            )
        
        conn.commit()
        conn.close()
        
        logger.info(f"Marked document as processed: {document_id}, success={is_processed}")
        return True
        
    except Exception as e:
        logger.error(f"Error marking document {document_id} as processed: {str(e)}")
        return False

def start_processing_run(model_name, notes=None):
    """
    Start a new processing run
    
    Args:
        model_name: Name of the model being used
        notes: Additional notes about the run
        
    Returns:
        Run ID if successful, None otherwise
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """
            INSERT INTO processing_runs (start_time, status, model_name, notes)
            VALUES (?, ?, ?, ?)
            """,
            (datetime.now(), "running", model_name, notes)
        )
        
        run_id = cursor.lastrowid
        
        conn.commit()
        conn.close()
        
        logger.info(f"Started processing run {run_id} with model {model_name}")
        return run_id
        
    except Exception as e:
        logger.error(f"Error starting processing run: {str(e)}")
        return None

def end_processing_run(run_id, status, documents_processed, errors_count):
    """
    End a processing run
    
    Args:
        run_id: ID of the processing run
        status: Final status of the run
        documents_processed: Number of documents processed
        errors_count: Number of errors encountered
        
    Returns:
        True if successful, False otherwise
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """
            UPDATE processing_runs
            SET end_time = ?,
                status = ?,
                documents_processed = ?,
                errors_count = ?
            WHERE run_id = ?
            """,
            (datetime.now(), status, documents_processed, errors_count, run_id)
        )
        
        conn.commit()
        conn.close()
        
        logger.info(f"Ended processing run {run_id}: {status}, {documents_processed} documents, {errors_count} errors")
        return True
        
    except Exception as e:
        logger.error(f"Error ending processing run {run_id}: {str(e)}")
        return False

def get_unprocessed_documents(state_code=None, limit=100):
    """
    Get unprocessed documents
    
    Args:
        state_code: Filter by state code (optional)
        limit: Maximum number of documents to return
        
    Returns:
        List of document rows
    """
    try:
        conn = get_connection(read_only=True)
        cursor = conn.cursor()
        
        if state_code:
            cursor.execute(
                """
                SELECT * FROM jurisdiction_documents
                WHERE is_processed = 0
                AND state_code = ?
                ORDER BY id
                LIMIT ?
                """,
                (state_code, limit)
            )
        else:
            cursor.execute(
                """
                SELECT * FROM jurisdiction_documents
                WHERE is_processed = 0
                ORDER BY id
                LIMIT ?
                """,
                (limit,)
            )
        
        results = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        
        logger.info(f"Retrieved {len(results)} unprocessed documents")
        return results
        
    except Exception as e:
        logger.error(f"Error getting unprocessed documents: {str(e)}")
        return []

def get_processing_statistics():
    """
    Get processing statistics
    
    Returns:
        Dictionary of statistics
    """
    try:
        conn = get_connection(read_only=True)
        cursor = conn.cursor()
        
        # Get document counts
        cursor.execute(
            """
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN is_processed = 1 THEN 1 ELSE 0 END) as processed,
                   COUNT(DISTINCT state_code) as states,
                   COUNT(DISTINCT place_id) as places
            FROM jurisdiction_documents
            """
        )
        doc_counts = dict(cursor.fetchone() or {"total": 0, "processed": 0, "states": 0, "places": 0})
        
        # Get document type counts
        cursor.execute(
            """
            SELECT document_type,
                   COUNT(*) as count,
                   SUM(CASE WHEN is_processed = 1 THEN 1 ELSE 0 END) as processed_count
            FROM jurisdiction_documents
            GROUP BY document_type
            ORDER BY count DESC
            """
        )
        doc_types = [dict(row) for row in cursor.fetchall()]
        
        # Get state stats
        cursor.execute(
            """
            SELECT s.state_code, s.state_name, s.document_count, s.processed_count,
                   CASE WHEN s.document_count > 0 
                        THEN (CAST(s.processed_count AS FLOAT) / CAST(s.document_count AS FLOAT)) * 100 
                        ELSE 0 END as percentage
            FROM states s
            WHERE s.document_count > 0
            ORDER BY s.document_count DESC
            """
        )
        state_stats = [dict(row) for row in cursor.fetchall()]
        
        # Get processing runs
        cursor.execute(
            """
            SELECT run_id, start_time, end_time, status, documents_processed, errors_count, model_name
            FROM processing_runs
            ORDER BY start_time DESC
            LIMIT 10
            """
        )
        recent_runs = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        
        # Calculate overall percentage
        overall_percentage = 0
        if doc_counts['total'] > 0:
            overall_percentage = (doc_counts['processed'] / doc_counts['total']) * 100
        
        # Assemble statistics
        stats = {
            "total_documents": doc_counts['total'],
            "processed_documents": doc_counts['processed'],
            "overall_percentage": overall_percentage,
            "states_count": doc_counts['states'],
            "places_count": doc_counts['places'],
            "document_types": doc_types,
            "state_stats": state_stats,
            "recent_runs": recent_runs
        }
        
        logger.info("Retrieved processing statistics")
        return stats
        
    except Exception as e:
        logger.error(f"Error getting processing statistics: {str(e)}")
        return {
            "total_documents": 0,
            "processed_documents": 0,
            "overall_percentage": 0,
            "states_count": 0,
            "places_count": 0,
            "document_types": [],
            "state_stats": [],
            "recent_runs": []
        }

def search_documents(query, state_code=None, document_type=None, is_processed=None, limit=100):
    """
    Search for documents in the database
    
    Args:
        query: Search query
        state_code: Filter by state code (optional)
        document_type: Filter by document type (optional)
        is_processed: Filter by processing status (optional)
        limit: Maximum number of documents to return
        
    Returns:
        List of document rows
    """
    try:
        conn = get_connection(read_only=True)
        cursor = conn.cursor()
        
        # Build the query
        sql = """
        SELECT jd.*, s.state_name, p.place_name
        FROM jurisdiction_documents jd
        LEFT JOIN states s ON jd.state_code = s.state_code
        LEFT JOIN places p ON jd.place_id = p.place_id
        WHERE 1=1
        """
        
        params = []
        
        if query:
            sql += " AND (jd.document_id LIKE ? OR p.place_name LIKE ?)"
            params.extend([f"%{query}%", f"%{query}%"])
        
        if state_code:
            sql += " AND jd.state_code = ?"
            params.append(state_code)
        
        if document_type:
            sql += " AND jd.document_type = ?"
            params.append(document_type)
        
        if is_processed is not None:
            sql += " AND jd.is_processed = ?"
            params.append(1 if is_processed else 0)
        
        sql += " ORDER BY jd.id LIMIT ?"
        params.append(limit)
        
        cursor.execute(sql, params)
        
        results = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        
        logger.info(f"Search returned {len(results)} results")
        return results
        
    except Exception as e:
        logger.error(f"Error searching documents: {str(e)}")
        return []

def backup_database():
    """
    Create a backup of the database
    
    Returns:
        Path to the backup file if successful, None otherwise
    """
    try:
        # Create backup filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(DB_BACKUP_DIR, f"american_law_data_{timestamp}.db")
        
        # Connect to the database
        conn = get_connection(read_only=True)
        
        # Create backup
        backup_conn = sqlite3.connect(backup_path)
        conn.backup(backup_conn)
        
        # Close connections
        backup_conn.close()
        conn.close()
        
        logger.info(f"Database backed up to {backup_path}")
        return backup_path
        
    except Exception as e:
        logger.error(f"Error backing up database: {str(e)}")
        return None

def export_to_csv(output_dir="exports"):
    """
    Export database tables to CSV files
    
    Args:
        output_dir: Directory to store the CSV files
        
    Returns:
        Dictionary with paths to the exported files if successful, None otherwise
    """
    try:
        os.makedirs(output_dir, exist_ok=True)
        
        conn = get_connection(read_only=True)
        
        # Tables to export
        tables = ["states", "places", "jurisdiction_documents", "processing_runs"]
        
        exported_files = {}
        
        for table in tables:
            # Read the table into a pandas DataFrame
            query = f"SELECT * FROM {table}"
            df = pd.read_sql_query(query, conn)
            
            # Create filename
            timestamp = datetime.now().strftime("%Y%m%d")
            filename = os.path.join(output_dir, f"{table}_{timestamp}.csv")
            
            # Export to CSV
            df.to_csv(filename, index=False)
            
            exported_files[table] = filename
        
        conn.close()
        
        logger.info(f"Database tables exported to {output_dir}")
        return exported_files
        
    except Exception as e:
        logger.error(f"Error exporting database to CSV: {str(e)}")
        return None

def main():
    """Main function for testing database utilities"""
    # Initialize the database
    initialize_database()
    
    # Add some sample data
    add_state("CA", "California")
    add_state("NY", "New York")
    
    add_place("CA_SF", "CA", "San Francisco", "city")
    add_place("CA_LA", "CA", "Los Angeles", "city")
    add_place("NY_NYC", "NY", "New York City", "city")
    
    # Add some sample documents
    add_document("doc001", "CA", "CA_SF", "ordinance", "2022-01-01", 
                 "data_cache/doc001.html", "data_cache/doc001_meta.json")
    add_document("doc002", "CA", "CA_LA", "regulation", "2022-02-01", 
                 "data_cache/doc002.html", "data_cache/doc002_meta.json")
    add_document("doc003", "NY", "NY_NYC", "statute", "2022-03-01", 
                 "data_cache/doc003.html", "data_cache/doc003_meta.json")
    
    # Mark a document as processed
    mark_document_processed("doc001", "processed/doc001.json", 2.5)
    
    # Get processing statistics
    stats = get_processing_statistics()
    print("Processing Statistics:")
    print(f"Total documents: {stats['total_documents']}")
    print(f"Processed documents: {stats['processed_documents']}")
    print(f"Completion: {stats['overall_percentage']:.2f}%")
    
    # Search for documents
    results = search_documents("San Francisco")
    print(f"\nFound {len(results)} documents matching 'San Francisco'")
    
    # Backup the database
    backup_path = backup_database()
    print(f"\nDatabase backed up to: {backup_path}")
    
    # Export to CSV
    exported_files = export_to_csv()
    print("\nExported database tables to:")
    for table, path in exported_files.items():
        print(f"  {table}: {path}")

if __name__ == "__main__":
    main() 