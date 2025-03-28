"""
Main processing workflow for the American Law Dataset.
Orchestrates the entire document processing pipeline.
"""

import os
import json
import logging
import time
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path

from tinycrops_lawyer.config import OUTPUT_DIR, DEFAULT_MODEL
from tinycrops_lawyer.database import manager as db_manager
from tinycrops_lawyer.schema import manager as schema_manager
from tinycrops_lawyer.data_loader import huggingface
from tinycrops_lawyer.parser.executor import MethodologyExecutor

# Initialize logger
logger = logging.getLogger(__name__)

def process_batch(limit: int = 100, state_code: Optional[str] = None, model_name: str = DEFAULT_MODEL) -> Dict:
    """
    Process a batch of documents from the American Law Dataset.
    
    Args:
        limit: Maximum number of documents to process
        state_code: Optional state code to filter by
        model_name: Gemini model to use
        
    Returns:
        Dict: Processing statistics
    """
    stats = {"processed": 0, "errors": 0}
    
    # Start processing run
    run_id = db_manager.start_processing_run(
        model_name=model_name,
        notes=f"State: {state_code if state_code else 'all'}, Limit: {limit}"
    )
    
    if not run_id:
        logger.error("Failed to start processing run")
        return stats
    
    try:
        # Get unprocessed documents
        documents = db_manager.get_unprocessed_documents(state_code=state_code, limit=limit)
        
        if not documents:
            logger.info("No unprocessed documents found")
            db_manager.end_processing_run(
                run_id=run_id,
                status="completed",
                documents_processed=0,
                errors_count=0
            )
            return stats
        
        logger.info(f"Processing {len(documents)} documents")
        
        # Initialize methodology executor
        executor = MethodologyExecutor(model_name=model_name)
        
        # Process each document
        for doc in documents:
            try:
                # Get document info
                document_id = doc['document_id']
                logger.info(f"Processing document {document_id}")
                
                # Get HTML content
                html_content = huggingface.get_document_html(document_id)
                
                if not html_content:
                    logger.error(f"Failed to get HTML content for document {document_id}")
                    stats["errors"] += 1
                    continue
                
                # Get schema ID
                schema_id = doc.get('schema_id')
                
                # Parse document
                parse_result = executor.parse(
                    document_id=document_id,
                    html_content=html_content,
                    schema_id=schema_id
                )
                
                if parse_result.success and parse_result.parsed_data:
                    # Organize output by state and place
                    state_dir = Path(OUTPUT_DIR) / "states" / (doc['state_code'] or "unknown")
                    place_dir = state_dir / (doc['place_id'] or "unknown")
                    doc_type_dir = place_dir / (doc['document_type'] or "unknown").lower()
                    
                    # Ensure directories exist
                    os.makedirs(doc_type_dir, exist_ok=True)
                    
                    # Save parsed data
                    output_path = doc_type_dir / f"{document_id}.json"
                    with open(output_path, 'w', encoding='utf-8') as f:
                        json.dump(parse_result.parsed_data, f, indent=2)
                    
                    # Mark document as processed
                    db_manager.mark_document_processed(
                        document_id=document_id,
                        processed_path=str(output_path),
                        processing_time=parse_result.processing_time
                    )
                    
                    stats["processed"] += 1
                    logger.info(f"Successfully processed document {document_id}")
                else:
                    # Mark document as processed with error
                    db_manager.mark_document_processed(
                        document_id=document_id,
                        processed_path="",
                        processing_time=parse_result.processing_time,
                        error=parse_result.error
                    )
                    
                    stats["errors"] += 1
                    logger.error(f"Failed to process document {document_id}: {parse_result.error}")
            
            except Exception as e:
                stats["errors"] += 1
                logger.error(f"Error processing document {doc['document_id']}: {e}")
        
        # End processing run
        db_manager.end_processing_run(
            run_id=run_id,
            status="completed",
            documents_processed=stats["processed"],
            errors_count=stats["errors"]
        )
        
        logger.info(f"Batch processing completed. Processed: {stats['processed']}, Errors: {stats['errors']}")
        
    except Exception as e:
        logger.error(f"Error during batch processing: {e}")
        
        # End processing run with error
        db_manager.end_processing_run(
            run_id=run_id,
            status="error",
            documents_processed=stats["processed"],
            errors_count=stats["errors"] + 1
        )
    
    return stats

def analyze_schemas(limit: int = 100) -> int:
    """
    Analyze unassigned documents to identify schemas.
    
    Args:
        limit: Maximum number of documents to analyze
        
    Returns:
        int: Number of documents analyzed
    """
    try:
        return schema_manager.batch_analyze_documents(limit=limit)
    except Exception as e:
        logger.error(f"Error analyzing schemas: {e}")
        return 0

def import_documents_from_metadata(state_code: Optional[str] = None) -> Dict:
    """
    Import documents from metadata files.
    
    Args:
        state_code: Optional state code to filter by
        
    Returns:
        Dict: Import statistics
    """
    stats = {"total": 0, "imported": 0, "states": 0, "places": 0}
    
    try:
        # Download metadata
        metadata = huggingface.batch_download_metadata()
        
        if not metadata:
            logger.error("Failed to download metadata")
            return stats
        
        stats["total"] = len(metadata)
        logger.info(f"Downloaded metadata for {len(metadata)} documents")
        
        # Extract jurisdiction info
        states = {}
        places = {}
        
        for doc_id, doc_info in metadata.items():
            # Skip if missing key information or if doc_info is not a dictionary
            if not doc_id or not isinstance(doc_info, dict):
                continue
            
            # Extract state information - safely get values
            doc_state_code = doc_info.get('state_code', '')
            doc_state_name = doc_info.get('state_name', '')
            doc_place_name = doc_info.get('place_name', '')
            
            # Skip documents without state or place info
            if not doc_state_code or not doc_place_name:
                continue
            
            # Skip if filtering by state code
            if state_code and doc_state_code != state_code:
                continue
            
            # Create state entry
            if doc_state_code not in states:
                states[doc_state_code] = {
                    'state_code': doc_state_code,
                    'state_name': doc_state_name
                }
            
            # Create place hash (for consistent IDs)
            import hashlib
            place_id = hashlib.md5(f"{doc_place_name}_{doc_state_code}".encode()).hexdigest()
            
            # Create place entry
            if place_id not in places:
                places[place_id] = {
                    'place_id': place_id,
                    'place_name': doc_place_name,
                    'state_code': doc_state_code
                }
            
            # Add document to database
            db_manager.add_document(
                document_id=doc_id,
                state_code=doc_state_code,
                place_id=place_id,
                document_type=doc_info.get('document_type', None),
                document_date=doc_info.get('date', None),
                html_path=f"american_law/html/{doc_id}.html"
            )
            
            stats["imported"] += 1
        
        # Add states and places to database
        for state_code, state_info in states.items():
            db_manager.add_state(state_code, state_info['state_name'])
        
        for place_id, place_info in places.items():
            db_manager.add_place(
                place_id=place_id,
                state_code=place_info['state_code'],
                place_name=place_info['place_name']
            )
        
        stats["states"] = len(states)
        stats["places"] = len(places)
        
        logger.info(f"Imported {stats['imported']} documents, {len(states)} states, {len(places)} places")
        
        return stats
    
    except Exception as e:
        logger.error(f"Error importing documents from metadata: {e}")
        return stats

def initialize_database_from_scratch() -> bool:
    """
    Initialize the database and import all metadata.
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Initialize database
        if not db_manager.initialize_database():
            logger.error("Failed to initialize database")
            return False
        
        # Import documents
        stats = import_documents_from_metadata()
        
        if stats["imported"] == 0:
            logger.error("Failed to import any documents")
            return False
        
        logger.info(f"Database initialized with {stats['imported']} documents")
        return True
    
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        return False

def reprocess_error_documents(limit: int = 100, model_name: str = DEFAULT_MODEL) -> Dict:
    """
    Reprocess documents that previously had errors.
    
    Args:
        limit: Maximum number of documents to reprocess
        model_name: Gemini model to use
        
    Returns:
        Dict: Processing statistics
    """
    stats = {"processed": 0, "errors": 0}
    
    try:
        # Get documents with errors
        session = db_manager.get_session()
        from tinycrops_lawyer.database.models import Document
        
        documents = session.query(Document).filter(
            Document.processing_error != None,
            Document.processing_error != ""
        ).limit(limit).all()
        session.close()
        
        if not documents:
            logger.info("No documents with errors found")
            return stats
        
        logger.info(f"Reprocessing {len(documents)} documents with errors")
        
        # Initialize methodology executor
        executor = MethodologyExecutor(model_name=model_name)
        
        # Process each document
        for doc in documents:
            document_id = doc.document_id
            
            # Get HTML content
            html_content = huggingface.get_document_html(document_id)
            
            if not html_content:
                logger.error(f"Failed to get HTML content for document {document_id}")
                stats["errors"] += 1
                continue
            
            # Parse document
            parse_result = executor.parse(
                document_id=document_id,
                html_content=html_content,
                schema_id=doc.schema_id
            )
            
            if parse_result.success and parse_result.parsed_data:
                # Organize output by state and place
                state_dir = Path(OUTPUT_DIR) / "states" / (doc.state_code or "unknown")
                place_dir = state_dir / (doc.place_id or "unknown")
                doc_type_dir = place_dir / (doc.document_type or "unknown").lower()
                
                # Ensure directories exist
                os.makedirs(doc_type_dir, exist_ok=True)
                
                # Save parsed data
                output_path = doc_type_dir / f"{document_id}.json"
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(parse_result.parsed_data, f, indent=2)
                
                # Mark document as processed
                db_manager.mark_document_processed(
                    document_id=document_id,
                    processed_path=str(output_path),
                    processing_time=parse_result.processing_time
                )
                
                stats["processed"] += 1
                logger.info(f"Successfully reprocessed document {document_id}")
            else:
                stats["errors"] += 1
                logger.error(f"Failed to reprocess document {document_id}: {parse_result.error}")
        
        logger.info(f"Reprocessing completed. Processed: {stats['processed']}, Errors: {stats['errors']}")
        
        return stats
    
    except Exception as e:
        logger.error(f"Error reprocessing documents: {e}")
        return stats 