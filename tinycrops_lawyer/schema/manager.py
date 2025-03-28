"""
Schema manager for HTML documents.
Handles schema operations, document grouping, and schema analysis.
"""

import os
import json
import logging
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
import shutil
from datetime import datetime

from tinycrops_lawyer.config import SCHEMA_CACHE_DIR
from tinycrops_lawyer.database import manager as db_manager
from tinycrops_lawyer.schema import analyzer
from tinycrops_lawyer.data_loader import huggingface

# Initialize logger
logger = logging.getLogger(__name__)

def get_or_create_schema(html_content: str) -> Tuple[str, Dict, bool]:
    """
    Get an existing schema or create a new one for the HTML content.
    
    Args:
        html_content: HTML content to analyze
        
    Returns:
        Tuple[str, Dict, bool]: Tuple containing:
            - schema_id: Schema identifier
            - schema_details: Schema details dictionary
            - is_new: True if a new schema was created, False if existing
    """
    # Extract schema signature
    schema_id, schema_details = analyzer.extract_schema_signature(html_content)
    if not schema_id:
        logger.error("Failed to extract schema signature")
        return None, None, False
    
    # Check if schema exists in database
    existing_schema = db_manager.get_schema(schema_id)
    is_new = existing_schema is None
    
    # Add or update schema in database
    db_manager.add_schema(schema_id, schema_details)
    
    return schema_id, schema_details, is_new

def analyze_document(
    document_id: str, 
    html_content: str, 
    metadata: Optional[Dict] = None
) -> Optional[str]:
    """
    Analyze a document and assign it to a schema.
    
    Args:
        document_id: Document identifier
        html_content: HTML content of the document
        metadata: Optional metadata dictionary
        
    Returns:
        Optional[str]: Schema ID or None if failed
    """
    try:
        # Get or create schema
        schema_id, schema_details, is_new = get_or_create_schema(html_content)
        if not schema_id:
            return None
        
        # Extract document type
        document_type = analyzer.extract_document_type(html_content, metadata)
        
        # Add document type to schema
        db_manager.add_document_type_to_schema(schema_id, document_type)
        
        # Update document with schema information
        if document_id:
            db_manager.add_document(
                document_id=document_id,
                state_code=metadata.get('state_code') if metadata else None,
                place_id=metadata.get('place_id') if metadata else None,
                document_type=document_type,
                document_date=metadata.get('date') if metadata else None,
                schema_id=schema_id
            )
        
        return schema_id
    
    except Exception as e:
        logger.error(f"Error analyzing document {document_id}: {e}")
        return None

def get_schema_sample_document(schema_id: str) -> Optional[Dict]:
    """
    Get a sample document for a given schema.
    
    Args:
        schema_id: Schema identifier
        
    Returns:
        Optional[Dict]: Sample document information or None if not found
    """
    try:
        # Query database for a document with this schema
        session = db_manager.get_session()
        from tinycrops_lawyer.database.models import Document
        
        document = session.query(Document).filter_by(schema_id=schema_id).first()
        session.close()
        
        if not document:
            logger.error(f"No documents found with schema ID {schema_id}")
            return None
        
        # Get document details
        doc_dict = db_manager.get_document(document.document_id)
        if not doc_dict:
            return None
        
        # Add HTML content
        html_content = None
        if doc_dict.get('html_path'):
            # Read directly from the path
            try:
                with open(doc_dict['html_path'], 'r', encoding='utf-8') as f:
                    html_content = f.read()
            except:
                pass
        
        # If reading from path failed, try fetching from HuggingFace
        if not html_content:
            html_content = huggingface.get_document_html(doc_dict['document_id'])
        
        if html_content:
            doc_dict['html_content'] = html_content
        
        return doc_dict
    
    except Exception as e:
        logger.error(f"Error getting sample document for schema {schema_id}: {e}")
        return None

def export_schema_samples(output_dir: Optional[str] = None) -> str:
    """
    Export sample documents for each schema.
    
    Args:
        output_dir: Optional directory to save samples
        
    Returns:
        str: Path to the directory with exported samples
    """
    if not output_dir:
        output_dir = os.path.join(SCHEMA_CACHE_DIR, "samples")
    
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        # Get all schemas
        session = db_manager.get_session()
        from tinycrops_lawyer.database.models import Schema
        
        schemas = session.query(Schema).all()
        session.close()
        
        schema_info = {}
        
        # Export a sample document for each schema
        for schema in schemas:
            schema_id = schema.schema_id
            schema_dir = os.path.join(output_dir, schema_id[:8])
            os.makedirs(schema_dir, exist_ok=True)
            
            sample_doc = get_schema_sample_document(schema_id)
            if not sample_doc or 'html_content' not in sample_doc:
                logger.warning(f"No sample document found for schema {schema_id}")
                continue
            
            # Save HTML sample
            html_path = os.path.join(schema_dir, f"sample.html")
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(sample_doc['html_content'])
            
            # Save document info
            info_path = os.path.join(schema_dir, f"document_info.json")
            doc_info = {k: v for k, v in sample_doc.items() if k != 'html_content'}
            with open(info_path, 'w', encoding='utf-8') as f:
                json.dump(doc_info, f, indent=2)
            
            # Save schema details
            schema_dict = db_manager.get_schema(schema_id)
            schema_path = os.path.join(schema_dir, f"schema_details.json")
            with open(schema_path, 'w', encoding='utf-8') as f:
                json.dump(schema_dict, f, indent=2)
            
            schema_info[schema_id] = {
                'document_count': schema.document_count,
                'sample_document_id': sample_doc['document_id'],
                'document_types': json.loads(schema.document_types) if schema.document_types else []
            }
        
        # Save schema registry
        registry_path = os.path.join(output_dir, "schema_registry.json")
        with open(registry_path, 'w', encoding='utf-8') as f:
            json.dump({
                'schemas': schema_info,
                'total_schemas': len(schema_info),
                'export_time': str(datetime.now())
            }, f, indent=2)
        
        logger.info(f"Exported {len(schema_info)} schema samples to {output_dir}")
        return output_dir
    
    except Exception as e:
        logger.error(f"Error exporting schema samples: {e}")
        return output_dir

def batch_analyze_documents(limit: int = 100) -> int:
    """
    Analyze a batch of documents to extract schemas.
    
    Args:
        limit: Maximum number of documents to analyze
        
    Returns:
        int: Number of documents analyzed
    """
    try:
        # Get documents that haven't been assigned a schema
        session = db_manager.get_session()
        from tinycrops_lawyer.database.models import Document
        
        documents = session.query(Document).filter_by(schema_id=None).limit(limit).all()
        session.close()
        
        analyzed_count = 0
        
        for doc in documents:
            # Get HTML content
            html_content = None
            if doc.html_path:
                try:
                    with open(doc.html_path, 'r', encoding='utf-8') as f:
                        html_content = f.read()
                except:
                    pass
            
            # If reading from path failed, try fetching from HuggingFace
            if not html_content:
                html_content = huggingface.get_document_html(doc.document_id)
            
            if not html_content:
                logger.warning(f"Could not get HTML content for document {doc.document_id}")
                continue
            
            # Get metadata
            metadata = {
                'document_type': doc.document_type,
                'state_code': doc.state_code,
                'place_id': doc.place_id,
                'date': doc.document_date
            }
            
            # Analyze document and assign schema
            schema_id = analyze_document(doc.document_id, html_content, metadata)
            if schema_id:
                analyzed_count += 1
        
        logger.info(f"Analyzed {analyzed_count} documents and assigned schemas")
        return analyzed_count
    
    except Exception as e:
        logger.error(f"Error in batch schema analysis: {e}")
        return 0

def get_schema_statistics() -> Dict:
    """
    Get statistics about schema distribution.
    
    Returns:
        Dict: Dictionary with schema statistics
    """
    try:
        session = db_manager.get_session()
        from tinycrops_lawyer.database.models import Schema, Document
        from sqlalchemy import func
        
        # Total schemas
        total_schemas = session.query(func.count(Schema.schema_id)).scalar() or 0
        
        # Total documents with schemas
        total_documents = session.query(func.count(Document.id)).filter(Document.schema_id != None).scalar() or 0
        
        # Document types with schemas
        doc_types_query = session.query(
            Document.document_type,
            func.count(Document.id).label('count')
        ).filter(Document.schema_id != None).group_by(Document.document_type).order_by(func.count(Document.id).desc())
        
        document_types = {row.document_type or 'unknown': row.count for row in doc_types_query}
        
        # Top schemas by document count
        top_schemas_query = session.query(
            Schema.schema_id,
            Schema.document_count,
            Schema.document_types
        ).order_by(Schema.document_count.desc()).limit(20)
        
        top_schemas = []
        for schema in top_schemas_query:
            schema_dict = {
                'schema_id': schema.schema_id,
                'short_id': schema.schema_id[:8],
                'document_count': schema.document_count,
                'document_types': json.loads(schema.document_types) if schema.document_types else []
            }
            top_schemas.append(schema_dict)
        
        session.close()
        
        return {
            'total_schemas': total_schemas,
            'total_documents_analyzed': total_documents,
            'document_types': document_types,
            'top_schemas': top_schemas
        }
    
    except Exception as e:
        logger.error(f"Error getting schema statistics: {e}")
        return {
            'total_schemas': 0,
            'total_documents_analyzed': 0,
            'document_types': {},
            'top_schemas': []
        } 