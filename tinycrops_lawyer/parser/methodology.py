"""
Models for representing parsing methodologies.
"""

import json
from typing import Dict, Optional, Any, List, Tuple
from datetime import datetime


class ParsingMethodology:
    """
    A parsing methodology for HTML documents.
    Contains executable Python code that can parse HTML documents with a specific schema.
    """
    
    def __init__(
        self,
        id: Optional[int] = None,
        schema_id: str = "",
        code: str = "",
        generated_at: Optional[datetime] = None,
        model_used: str = "",
        is_active: bool = True,
        performance_stats: Optional[Dict] = None
    ):
        """
        Initialize a parsing methodology.
        
        Args:
            id: Optional ID from the database
            schema_id: Schema identifier
            code: Python code string
            generated_at: Timestamp when the methodology was generated
            model_used: Model used to generate the methodology
            is_active: Whether this methodology is active
            performance_stats: Performance statistics for this methodology
        """
        self.id = id
        self.schema_id = schema_id
        self.code = code
        self.generated_at = generated_at or datetime.now()
        self.model_used = model_used
        self.is_active = is_active
        self.performance_stats = performance_stats or {
            "success_count": 0,
            "error_count": 0,
            "avg_processing_time": 0.0
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "ParsingMethodology":
        """
        Create a methodology from a dictionary.
        
        Args:
            data: Dictionary with methodology data
            
        Returns:
            ParsingMethodology: A new instance
        """
        return cls(
            id=data.get('id'),
            schema_id=data.get('schema_id', ''),
            code=data.get('methodology_code', ''),
            generated_at=datetime.fromisoformat(data['generated_at']) if data.get('generated_at') else None,
            model_used=data.get('model_used', ''),
            is_active=data.get('is_active', True),
            performance_stats=data.get('performance_stats', {})
        )
    
    def to_dict(self) -> Dict:
        """
        Convert this methodology to a dictionary.
        
        Returns:
            Dict: Dictionary representation
        """
        return {
            'id': self.id,
            'schema_id': self.schema_id,
            'methodology_code': self.code,
            'generated_at': self.generated_at.isoformat(),
            'model_used': self.model_used,
            'is_active': self.is_active,
            'performance_stats': self.performance_stats
        }
    
    def __str__(self) -> str:
        """String representation of the methodology."""
        return f"ParsingMethodology(id={self.id}, schema_id={self.schema_id}, model={self.model_used})"


class ParseResult:
    """
    Result of parsing an HTML document.
    """
    
    def __init__(
        self,
        document_id: str,
        success: bool,
        parsed_data: Optional[Dict] = None,
        error: Optional[str] = None,
        methodology_id: Optional[int] = None,
        processing_time: Optional[float] = None
    ):
        """
        Initialize a parse result.
        
        Args:
            document_id: Document identifier
            success: Whether parsing was successful
            parsed_data: Parsed document structure
            error: Error message if parsing failed
            methodology_id: ID of the methodology used
            processing_time: Time taken to parse in seconds
        """
        self.document_id = document_id
        self.success = success
        self.parsed_data = parsed_data
        self.error = error
        self.methodology_id = methodology_id
        self.processing_time = processing_time
    
    def to_dict(self) -> Dict:
        """
        Convert this result to a dictionary.
        
        Returns:
            Dict: Dictionary representation
        """
        return {
            'document_id': self.document_id,
            'success': self.success,
            'parsed_data': self.parsed_data,
            'error': self.error,
            'methodology_id': self.methodology_id,
            'processing_time': self.processing_time
        }
    
    def __str__(self) -> str:
        """String representation of the parse result."""
        status = "SUCCESS" if self.success else "FAILED"
        return f"ParseResult({status}, document_id={self.document_id}, methodology_id={self.methodology_id})" 