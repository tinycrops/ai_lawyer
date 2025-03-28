"""
Executor for running parsing methodologies on HTML documents.
"""

import time
import logging
import sys
import json
from typing import Dict, Optional, Any, List, Tuple

from bs4 import BeautifulSoup

from tinycrops_lawyer.database import manager as db_manager
from tinycrops_lawyer.schema import manager as schema_manager
from tinycrops_lawyer.parser import gemini
from tinycrops_lawyer.parser.methodology import ParsingMethodology, ParseResult

# Initialize logger
logger = logging.getLogger(__name__)

class MethodologyExecutor:
    """
    Manages and executes parsing methodologies for HTML documents.
    This class is responsible for retrieving, generating, and executing methodologies.
    """
    
    def __init__(self, model_name: str = "gemini-2.0-flash"):
        """
        Initialize the methodology executor.
        
        Args:
            model_name: Name of the Gemini model to use
        """
        self.model_name = model_name
        
        # Ensure BeautifulSoup is available in the global namespace for methodologies
        globals()['BeautifulSoup'] = BeautifulSoup
    
    def parse(self, document_id: str, html_content: str, schema_id: Optional[str] = None) -> ParseResult:
        """
        Parse an HTML document using a methodology for its schema.
        If no methodology exists, one will be generated. If that fails,
        fallback to direct parsing with Gemini.
        
        Args:
            document_id: Document identifier
            html_content: HTML content to parse
            schema_id: Optional schema ID. If not provided, will be detected
            
        Returns:
            ParseResult: Result of the parsing operation
        """
        start_time = time.time()
        
        try:
            # If schema_id not provided, analyze document to get schema
            if not schema_id:
                document_info = db_manager.get_document(document_id)
                schema_id = document_info.get('schema_id') if document_info else None
                
                # If still no schema, analyze document content
                if not schema_id:
                    schema_id = schema_manager.analyze_document(document_id, html_content)
                    
                    if not schema_id:
                        logger.error(f"Could not determine schema for document {document_id}")
                        return ParseResult(
                            document_id=document_id,
                            success=False,
                            error="Could not determine document schema",
                            processing_time=time.time() - start_time
                        )
            
            # Get methodology for this schema
            methodology = self.get_or_generate_methodology(schema_id, html_content)
            
            if methodology:
                # Execute the methodology
                result = self.execute_methodology(methodology, html_content, document_id)
                
                # Update methodology performance stats
                if methodology.id:
                    db_manager.update_methodology_performance(
                        methodology_id=methodology.id,
                        success=result.success,
                        processing_time=result.processing_time
                    )
                
                return result
            else:
                logger.warning(f"No methodology for schema {schema_id}, using direct Gemini parsing")
                # Fallback to direct parsing with Gemini
                return self.parse_with_gemini_fallback(document_id, html_content)
        
        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"Error in parse process for document {document_id}: {e}")
            return ParseResult(
                document_id=document_id,
                success=False,
                error=str(e),
                processing_time=processing_time
            )
    
    def get_or_generate_methodology(self, schema_id: str, sample_html: str) -> Optional[ParsingMethodology]:
        """
        Get an existing methodology for a schema or generate a new one.
        
        Args:
            schema_id: Schema identifier
            sample_html: Sample HTML content for methodology generation
            
        Returns:
            Optional[ParsingMethodology]: Methodology or None if failed
        """
        try:
            # Try to get existing methodology from database
            methodology_dict = db_manager.get_parsing_methodology(schema_id)
            
            if methodology_dict and methodology_dict.get('methodology_code'):
                # Use existing methodology
                logger.info(f"Using existing methodology for schema {schema_id}")
                return ParsingMethodology.from_dict(methodology_dict)
            
            # No existing methodology, generate new one
            logger.info(f"Generating new methodology for schema {schema_id}")
            
            # Get schema details
            schema_details = db_manager.get_schema(schema_id)
            if not schema_details:
                logger.error(f"Schema {schema_id} not found")
                return None
            
            # Generate methodology
            code = gemini.generate_parsing_methodology(
                schema_signature=schema_details.get('signature', {}),
                sample_html=sample_html,
                model_name=self.model_name
            )
            
            if not code:
                logger.error(f"Failed to generate methodology for schema {schema_id}")
                return None
            
            # Test the methodology
            success, _, error = gemini.test_parsing_methodology(code, sample_html)
            if not success:
                logger.error(f"Generated methodology failed testing: {error}")
                return None
            
            # Save to database
            methodology_id = db_manager.add_parsing_methodology(
                schema_id=schema_id,
                methodology_code=code,
                model_used=self.model_name
            )
            
            if not methodology_id:
                logger.error(f"Failed to save methodology for schema {schema_id}")
                return None
            
            return ParsingMethodology(
                id=methodology_id,
                schema_id=schema_id,
                code=code,
                model_used=self.model_name
            )
        
        except Exception as e:
            logger.error(f"Error getting/generating methodology for schema {schema_id}: {e}")
            return None
    
    def execute_methodology(self, methodology: ParsingMethodology, html_content: str, document_id: str) -> ParseResult:
        """
        Execute a parsing methodology on an HTML document.
        
        Args:
            methodology: Parsing methodology to execute
            html_content: HTML content to parse
            document_id: Document identifier
            
        Returns:
            ParseResult: Result of the parsing operation
        """
        start_time = time.time()
        
        try:
            success, result, error = gemini.test_parsing_methodology(methodology.code, html_content)
            
            if not success:
                logger.error(f"Error executing methodology for document {document_id}: {error}")
                return ParseResult(
                    document_id=document_id,
                    success=False,
                    error=error,
                    methodology_id=methodology.id,
                    processing_time=time.time() - start_time
                )
            
            # Set document ID in parsed data
            if result and isinstance(result, dict):
                result['document_id'] = document_id
            
            processing_time = time.time() - start_time
            return ParseResult(
                document_id=document_id,
                success=True,
                parsed_data=result,
                methodology_id=methodology.id,
                processing_time=processing_time
            )
            
        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"Exception executing methodology for document {document_id}: {e}")
            return ParseResult(
                document_id=document_id,
                success=False,
                error=str(e),
                methodology_id=methodology.id,
                processing_time=processing_time
            )
    
    def parse_with_gemini_fallback(self, document_id: str, html_content: str) -> ParseResult:
        """
        Parse a document directly with Gemini as a fallback.
        
        Args:
            document_id: Document identifier
            html_content: HTML content to parse
            
        Returns:
            ParseResult: Result of the parsing operation
        """
        start_time = time.time()
        
        try:
            # Get document info
            document_info = db_manager.get_document(document_id) or {'document_id': document_id}
            
            # Parse with Gemini
            parsed_data = gemini.parse_document_with_gemini(
                html_content=html_content,
                document_info=document_info,
                model_name=self.model_name
            )
            
            if not parsed_data:
                return ParseResult(
                    document_id=document_id,
                    success=False,
                    error="Gemini parsing failed to return valid data",
                    processing_time=time.time() - start_time
                )
            
            # Set document ID in parsed data
            if isinstance(parsed_data, dict):
                parsed_data['document_id'] = document_id
            
            processing_time = time.time() - start_time
            return ParseResult(
                document_id=document_id,
                success=True,
                parsed_data=parsed_data,
                processing_time=processing_time
            )
            
        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"Exception in Gemini fallback parsing for document {document_id}: {e}")
            return ParseResult(
                document_id=document_id,
                success=False,
                error=str(e),
                processing_time=processing_time
            ) 