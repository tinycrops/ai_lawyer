"""
Database manager for the American Law Dataset.
Handles session management, database operations, and CRUD functions.
"""

import os
import json
import logging
import shutil
import pandas as pd
from datetime import datetime
from pathlib import Path
from sqlalchemy import create_engine, func, text
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import SQLAlchemyError
from typing import Dict, List, Optional, Any, Tuple

from tinycrops_lawyer.config import DB_PATH, DB_BACKUP_DIR
from tinycrops_lawyer.database.models import Base, State, Place, Document, Schema, ParsingMethodology, ProcessingRun

# Initialize logger
logger = logging.getLogger(__name__)

# Create the engine
engine = create_engine(f"sqlite:///{DB_PATH}")

# Create session factory
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

def get_session():
    """
    Get a scoped session to the database.
    
    Returns:
        SQLAlchemy session object
    """
    return Session()

def initialize_database() -> bool:
    """
    Initialize the database and create tables if they don't exist.
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        Base.metadata.create_all(engine)
        logger.info("Database initialized successfully")
        return True
    except SQLAlchemyError as e:
        logger.error(f"Error initializing database: {e}")
        return False

def backup_database() -> Optional[str]:
    """
    Create a backup of the database.
    
    Returns:
        Optional[str]: Path to the backup file or None if failed
    """
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = Path(DB_BACKUP_DIR) / f"american_law_backup_{timestamp}.db"
        shutil.copy2(DB_PATH, backup_path)
        logger.info(f"Database backed up to {backup_path}")
        return str(backup_path)
    except Exception as e:
        logger.error(f"Error backing up database: {e}")
        return None

# ============= State Operations =============

def add_state(state_code: str, state_name: str) -> bool:
    """
    Add a state or update if it already exists.
    
    Args:
        state_code: Two-letter state code
        state_name: Full state name
        
    Returns:
        bool: True if successful, False otherwise
    """
    session = get_session()
    try:
        state = session.query(State).filter_by(state_code=state_code).first()
        if state:
            state.state_name = state_name
            state.last_updated = datetime.now()
        else:
            state = State(
                state_code=state_code,
                state_name=state_name,
                last_updated=datetime.now()
            )
            session.add(state)
        
        session.commit()
        logger.info(f"Added/updated state: {state_code} - {state_name}")
        return True
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Error adding state {state_code}: {e}")
        return False
    finally:
        session.close()

def get_state(state_code: str) -> Optional[Dict]:
    """
    Get state by code.
    
    Args:
        state_code: Two-letter state code
        
    Returns:
        Optional[Dict]: State info dictionary or None if not found
    """
    session = get_session()
    try:
        state = session.query(State).filter_by(state_code=state_code).first()
        if state:
            return {
                'state_code': state.state_code,
                'state_name': state.state_name,
                'document_count': state.document_count,
                'processed_count': state.processed_count,
                'last_updated': state.last_updated.isoformat() if state.last_updated else None
            }
        return None
    except SQLAlchemyError as e:
        logger.error(f"Error retrieving state {state_code}: {e}")
        return None
    finally:
        session.close()

def get_all_states() -> List[Dict]:
    """
    Get all states with their statistics.
    
    Returns:
        List[Dict]: List of state dictionaries
    """
    session = get_session()
    try:
        states = session.query(State).order_by(State.state_name).all()
        return [
            {
                'state_code': s.state_code,
                'state_name': s.state_name,
                'document_count': s.document_count,
                'processed_count': s.processed_count,
                'percentage': round((s.processed_count / s.document_count * 100) 
                              if s.document_count > 0 else 0, 2)
            }
            for s in states
        ]
    except SQLAlchemyError as e:
        logger.error(f"Error retrieving states: {e}")
        return []
    finally:
        session.close()

# ============= Place Operations =============

def add_place(place_id: str, state_code: str, place_name: str, place_type: Optional[str] = None) -> bool:
    """
    Add a place or update if it already exists.
    
    Args:
        place_id: Unique identifier for the place
        state_code: Two-letter state code
        place_name: Name of the place
        place_type: Type of place (e.g., city, county)
        
    Returns:
        bool: True if successful, False otherwise
    """
    session = get_session()
    try:
        place = session.query(Place).filter_by(place_id=place_id).first()
        if place:
            place.state_code = state_code
            place.place_name = place_name
            place.place_type = place_type
            place.last_updated = datetime.now()
        else:
            place = Place(
                place_id=place_id,
                state_code=state_code,
                place_name=place_name,
                place_type=place_type,
                last_updated=datetime.now()
            )
            session.add(place)
        
        session.commit()
        logger.info(f"Added/updated place: {place_name} ({place_id}) in {state_code}")
        return True
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Error adding place {place_id}: {e}")
        return False
    finally:
        session.close()

# ============= Document Operations =============

def add_document(
    document_id: str, 
    state_code: str, 
    place_id: str, 
    document_type: Optional[str] = None, 
    document_date: Optional[str] = None,
    html_path: Optional[str] = None, 
    metadata_path: Optional[str] = None,
    schema_id: Optional[str] = None
) -> bool:
    """
    Add a document or update if it already exists.
    
    Args:
        document_id: Unique identifier for the document
        state_code: Two-letter state code
        place_id: Place identifier
        document_type: Type of document
        document_date: Date of the document
        html_path: Path to the HTML file
        metadata_path: Path to the metadata file
        schema_id: Schema identifier
        
    Returns:
        bool: True if successful, False otherwise
    """
    session = get_session()
    try:
        document = session.query(Document).filter_by(document_id=document_id).first()
        if document:
            document.state_code = state_code
            document.place_id = place_id
            document.document_type = document_type
            document.document_date = document_date
            document.html_path = html_path
            document.metadata_path = metadata_path
            document.schema_id = schema_id
        else:
            document = Document(
                document_id=document_id,
                state_code=state_code,
                place_id=place_id,
                document_type=document_type,
                document_date=document_date,
                html_path=html_path,
                metadata_path=metadata_path,
                schema_id=schema_id
            )
            session.add(document)
            
            # Increment document counts
            state = session.query(State).filter_by(state_code=state_code).first()
            if state:
                state.document_count += 1
                
            place = session.query(Place).filter_by(place_id=place_id).first()
            if place:
                place.document_count += 1
        
        session.commit()
        logger.info(f"Added/updated document: {document_id}")
        return True
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Error adding document {document_id}: {e}")
        return False
    finally:
        session.close()

def get_document(document_id: str) -> Optional[Dict]:
    """
    Get document by ID.
    
    Args:
        document_id: Document identifier
        
    Returns:
        Optional[Dict]: Document dictionary or None if not found
    """
    session = get_session()
    try:
        document = session.query(Document).filter_by(document_id=document_id).first()
        if not document:
            return None
            
        return {
            'document_id': document.document_id,
            'state_code': document.state_code,
            'place_id': document.place_id,
            'document_type': document.document_type,
            'document_date': document.document_date,
            'html_path': document.html_path,
            'metadata_path': document.metadata_path,
            'schema_id': document.schema_id,
            'is_processed': document.is_processed,
            'processed_path': document.processed_path,
            'processed_date': document.processed_date.isoformat() if document.processed_date else None,
            'processing_time': document.processing_time,
            'processing_error': document.processing_error
        }
    except SQLAlchemyError as e:
        logger.error(f"Error retrieving document {document_id}: {e}")
        return None
    finally:
        session.close()

def get_unprocessed_documents(state_code: Optional[str] = None, limit: int = 100) -> List[Dict]:
    """
    Get unprocessed documents, optionally filtered by state.
    
    Args:
        state_code: Optional filter for state code
        limit: Maximum number of documents to return
        
    Returns:
        List[Dict]: List of document dictionaries
    """
    session = get_session()
    try:
        query = session.query(Document).filter_by(is_processed=False)
        
        if state_code:
            query = query.filter_by(state_code=state_code)
        
        documents = query.limit(limit).all()
        
        return [
            {
                'document_id': doc.document_id,
                'state_code': doc.state_code,
                'place_id': doc.place_id,
                'document_type': doc.document_type,
                'document_date': doc.document_date,
                'html_path': doc.html_path,
                'metadata_path': doc.metadata_path,
                'schema_id': doc.schema_id
            }
            for doc in documents
        ]
    except SQLAlchemyError as e:
        logger.error(f"Error retrieving unprocessed documents: {e}")
        return []
    finally:
        session.close()

def mark_document_processed(
    document_id: str, 
    processed_path: str, 
    processing_time: Optional[float] = None, 
    error: Optional[str] = None
) -> bool:
    """
    Mark a document as processed.
    
    Args:
        document_id: Document identifier
        processed_path: Path to the processed output file
        processing_time: Time taken to process in seconds
        error: Error message if processing failed
        
    Returns:
        bool: True if successful, False otherwise
    """
    session = get_session()
    try:
        document = session.query(Document).filter_by(document_id=document_id).first()
        if not document:
            logger.error(f"Document {document_id} not found")
            return False
            
        document.is_processed = True
        document.processed_path = processed_path
        document.processed_date = datetime.now()
        document.processing_time = processing_time
        document.processing_error = error
        
        # Update counts if there was no error
        if not error:
            # Update state processed count
            if document.state_code:
                state = session.query(State).filter_by(state_code=document.state_code).first()
                if state:
                    state.processed_count += 1
                    
            # Update place processed count
            if document.place_id:
                place = session.query(Place).filter_by(place_id=document.place_id).first()
                if place:
                    place.processed_count += 1
        
        session.commit()
        logger.info(f"Marked document {document_id} as processed")
        return True
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Error marking document {document_id} as processed: {e}")
        return False
    finally:
        session.close()

# ============= Schema Operations =============

def add_schema(schema_id: str, signature: Dict) -> bool:
    """
    Add a schema or update if it already exists.
    
    Args:
        schema_id: Schema hash/identifier
        signature: Schema structure details
        
    Returns:
        bool: True if successful, False otherwise
    """
    session = get_session()
    try:
        schema = session.query(Schema).filter_by(schema_id=schema_id).first()
        if schema:
            schema.last_seen = datetime.now()
            schema.document_count += 1
        else:
            schema = Schema(
                schema_id=schema_id,
                signature=json.dumps(signature),
                document_count=1,
                document_types=json.dumps([]),
                first_seen=datetime.now(),
                last_seen=datetime.now()
            )
            session.add(schema)
        
        session.commit()
        logger.info(f"Added/updated schema: {schema_id}")
        return True
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Error adding schema {schema_id}: {e}")
        return False
    finally:
        session.close()

def get_schema(schema_id: str) -> Optional[Dict]:
    """
    Get schema by ID.
    
    Args:
        schema_id: Schema identifier
        
    Returns:
        Optional[Dict]: Schema dictionary or None if not found
    """
    session = get_session()
    try:
        schema = session.query(Schema).filter_by(schema_id=schema_id).first()
        if not schema:
            return None
            
        return {
            'schema_id': schema.schema_id,
            'signature': json.loads(schema.signature) if schema.signature else {},
            'document_count': schema.document_count,
            'document_types': json.loads(schema.document_types) if schema.document_types else [],
            'first_seen': schema.first_seen.isoformat() if schema.first_seen else None,
            'last_seen': schema.last_seen.isoformat() if schema.last_seen else None
        }
    except SQLAlchemyError as e:
        logger.error(f"Error retrieving schema {schema_id}: {e}")
        return None
    finally:
        session.close()

def add_document_type_to_schema(schema_id: str, document_type: str) -> bool:
    """
    Add a document type to the schema's list of document types.
    
    Args:
        schema_id: Schema identifier
        document_type: Document type to add
        
    Returns:
        bool: True if successful, False otherwise
    """
    session = get_session()
    try:
        schema = session.query(Schema).filter_by(schema_id=schema_id).first()
        if not schema:
            logger.error(f"Schema {schema_id} not found")
            return False
            
        doc_types = json.loads(schema.document_types) if schema.document_types else []
        if document_type not in doc_types:
            doc_types.append(document_type)
            schema.document_types = json.dumps(doc_types)
            session.commit()
            logger.info(f"Added document type {document_type} to schema {schema_id}")
        
        return True
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Error adding document type to schema {schema_id}: {e}")
        return False
    finally:
        session.close()

# ============= Parsing Methodology Operations =============

def add_parsing_methodology(
    schema_id: str, 
    methodology_code: str, 
    model_used: str
) -> Optional[int]:
    """
    Add a parsing methodology for a schema.
    
    Args:
        schema_id: Schema identifier
        methodology_code: Python code string of the methodology
        model_used: Name of the model used to generate the methodology
        
    Returns:
        Optional[int]: ID of the new methodology or None if failed
    """
    session = get_session()
    try:
        # Deactivate existing methodologies for this schema
        existing_methods = session.query(ParsingMethodology).filter_by(
            schema_id=schema_id,
            is_active=True
        ).all()
        
        for method in existing_methods:
            method.is_active = False
        
        # Add new methodology
        methodology = ParsingMethodology(
            schema_id=schema_id,
            methodology_code=methodology_code,
            generated_at=datetime.now(),
            model_used=model_used,
            is_active=True,
            performance_stats=json.dumps({
                "success_count": 0,
                "error_count": 0,
                "avg_processing_time": 0.0
            })
        )
        session.add(methodology)
        session.commit()
        
        logger.info(f"Added new parsing methodology for schema {schema_id}")
        return methodology.id
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Error adding parsing methodology for schema {schema_id}: {e}")
        return None
    finally:
        session.close()

def get_parsing_methodology(schema_id: str) -> Optional[Dict]:
    """
    Get the active parsing methodology for a schema.
    
    Args:
        schema_id: Schema identifier
        
    Returns:
        Optional[Dict]: Methodology dictionary or None if not found
    """
    session = get_session()
    try:
        methodology = session.query(ParsingMethodology).filter_by(
            schema_id=schema_id,
            is_active=True
        ).order_by(ParsingMethodology.generated_at.desc()).first()
        
        if not methodology:
            return None
            
        return {
            'id': methodology.id,
            'schema_id': methodology.schema_id,
            'methodology_code': methodology.methodology_code,
            'generated_at': methodology.generated_at.isoformat(),
            'model_used': methodology.model_used,
            'performance_stats': json.loads(methodology.performance_stats) if methodology.performance_stats else {}
        }
    except SQLAlchemyError as e:
        logger.error(f"Error retrieving parsing methodology for schema {schema_id}: {e}")
        return None
    finally:
        session.close()

def update_methodology_performance(
    methodology_id: int, 
    success: bool, 
    processing_time: Optional[float] = None
) -> bool:
    """
    Update the performance statistics of a methodology.
    
    Args:
        methodology_id: Methodology identifier
        success: Whether the methodology successfully parsed a document
        processing_time: Processing time in seconds
        
    Returns:
        bool: True if successful, False otherwise
    """
    session = get_session()
    try:
        methodology = session.query(ParsingMethodology).filter_by(id=methodology_id).first()
        if not methodology:
            logger.error(f"Methodology {methodology_id} not found")
            return False
            
        stats = json.loads(methodology.performance_stats) if methodology.performance_stats else {
            "success_count": 0,
            "error_count": 0,
            "avg_processing_time": 0.0
        }
        
        # Update counts
        if success:
            stats["success_count"] += 1
        else:
            stats["error_count"] += 1
            
        # Update average processing time
        if processing_time is not None:
            total_count = stats["success_count"] + stats["error_count"]
            old_avg = stats["avg_processing_time"]
            
            if total_count > 1:
                # Weighted average calculation
                stats["avg_processing_time"] = ((old_avg * (total_count - 1)) + processing_time) / total_count
            else:
                stats["avg_processing_time"] = processing_time
        
        methodology.performance_stats = json.dumps(stats)
        session.commit()
        return True
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Error updating methodology performance {methodology_id}: {e}")
        return False
    finally:
        session.close()

# ============= Processing Run Operations =============

def start_processing_run(model_name: str, notes: Optional[str] = None) -> Optional[int]:
    """
    Start a new processing run.
    
    Args:
        model_name: Name of the model being used
        notes: Additional notes about the run
        
    Returns:
        Optional[int]: Run ID or None if failed
    """
    session = get_session()
    try:
        run = ProcessingRun(
            start_time=datetime.now(),
            status="running",
            model_name=model_name,
            notes=notes
        )
        session.add(run)
        session.commit()
        
        logger.info(f"Started processing run {run.run_id}")
        return run.run_id
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Error starting processing run: {e}")
        return None
    finally:
        session.close()

def end_processing_run(
    run_id: int, 
    status: str, 
    documents_processed: int, 
    errors_count: int
) -> bool:
    """
    End a processing run with final statistics.
    
    Args:
        run_id: Run identifier
        status: Final status (completed, error, etc.)
        documents_processed: Number of documents processed
        errors_count: Number of errors encountered
        
    Returns:
        bool: True if successful, False otherwise
    """
    session = get_session()
    try:
        run = session.query(ProcessingRun).filter_by(run_id=run_id).first()
        if not run:
            logger.error(f"Processing run {run_id} not found")
            return False
            
        run.end_time = datetime.now()
        run.status = status
        run.documents_processed = documents_processed
        run.errors_count = errors_count
        
        session.commit()
        logger.info(f"Ended processing run {run_id} with status {status}")
        return True
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Error ending processing run {run_id}: {e}")
        return False
    finally:
        session.close()

# ============= Statistics and Reporting =============

def get_processing_statistics():
    """
    Get overall processing statistics.
    
    Returns:
        dict: Dictionary with statistics about document processing
    """
    session = get_session()
    from tinycrops_lawyer.database.models import Document, State, Place
    from sqlalchemy import func
    
    # Get document counts
    total_documents = session.query(Document).count()
    processed_documents = session.query(Document).filter(Document.is_processed == True).count()
    
    # Get state and place counts
    states_count = session.query(State).count()
    places_count = session.query(Place).count()
    
    # Get document types
    doc_types_query = (
        session.query(
            Document.document_type,
            func.count(Document.id).label('count')
        )
        .group_by(Document.document_type)
        .order_by(func.count(Document.id).desc())
    )
    
    document_types = [
        {
            'document_type': doc_type or 'Unknown',
            'count': count
        }
        for doc_type, count in doc_types_query
    ]
    
    # Calculate overall percentage
    overall_percentage = 0
    if total_documents > 0:
        overall_percentage = round(processed_documents / total_documents * 100)
    
    session.close()
    
    return {
        'total_documents': total_documents,
        'processed_documents': processed_documents,
        'states_count': states_count,
        'places_count': places_count,
        'overall_percentage': overall_percentage,
        'document_types': document_types
    }

def get_all_states():
    """
    Get all states with counts.
    
    Returns:
        list: List of state dictionaries with counts
    """
    session = get_session()
    states = []
    
    for state in session.query(State).all():
        states.append({
            'state_code': state.state_code,
            'state_name': state.state_name,
            'document_count': state.document_count,
            'processed_count': state.processed_count,
            'places_count': state.places_count
        })
    
    session.close()
    return states

def get_state(state_code):
    """
    Get a specific state with counts.
    
    Args:
        state_code: The state code (e.g., 'CA', 'NY')
        
    Returns:
        dict: State dictionary with counts or None if not found
    """
    session = get_session()
    state = session.query(State).filter_by(state_code=state_code).first()
    
    if not state:
        session.close()
        return None
    
    result = {
        'state_code': state.state_code,
        'state_name': state.state_name,
        'document_count': state.document_count,
        'processed_count': state.processed_count,
        'places_count': state.places_count
    }
    
    session.close()
    return result

def export_to_csv(output_dir: str = "exports") -> Optional[Dict[str, str]]:
    """
    Export all database tables to CSV files.
    
    Args:
        output_dir: Directory to save the CSV files
        
    Returns:
        Optional[Dict[str, str]]: Dictionary mapping table names to file paths
    """
    try:
        os.makedirs(output_dir, exist_ok=True)
        conn = engine.connect()
        
        tables = {
            'states': State.__tablename__,
            'places': Place.__tablename__,
            'documents': Document.__tablename__,
            'schemas': Schema.__tablename__,
            'methodologies': ParsingMethodology.__tablename__,
            'runs': ProcessingRun.__tablename__
        }
        
        exported_files = {}
        
        for name, table in tables.items():
            file_path = os.path.join(output_dir, f"{table}.csv")
            df = pd.read_sql_table(table, conn)
            df.to_csv(file_path, index=False)
            exported_files[name] = file_path
            
        conn.close()
        logger.info(f"Exported database tables to {output_dir}")
        return exported_files
    except Exception as e:
        logger.error(f"Error exporting database to CSV: {e}")
        return None 