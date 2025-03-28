"""
Schema analyzer for HTML documents.
Identifies structural patterns in HTML to group similar documents.
"""

import hashlib
import json
import logging
import re
from typing import Dict, Tuple, List, Optional, Any
from collections import Counter

from bs4 import BeautifulSoup

# Initialize logger
logger = logging.getLogger(__name__)

def extract_schema_signature(html_content: str) -> Tuple[str, Dict]:
    """
    Extract a schema signature from HTML content.
    
    Args:
        html_content: HTML content to analyze
        
    Returns:
        Tuple[str, Dict]: A tuple containing:
            - signature hash (str): A unique identifier for the schema
            - signature details (Dict): Structure information about the schema
    """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Count tag frequencies
        tag_counts = Counter([tag.name for tag in soup.find_all() if tag.name])
        
        # Extract basic structure (first 50 elements)
        structure = [tag.name for tag in list(soup.descendants)[:100] if hasattr(tag, 'name') and tag.name]
        
        # Get structural element patterns
        has_tables = len(soup.find_all('table')) > 0
        has_lists = len(soup.find_all(['ul', 'ol'])) > 0
        has_sections = bool(re.search(r'section|§|\bsec\b|\bart\b|article', soup.get_text().lower()))
        
        # Analysis of hierarchical structure
        hierarchy_patterns = []
        for tag in soup.find_all(True)[:200]:  # Limit to first 200 tags for performance
            if tag.parent and tag.parent.name:
                pattern = f"{tag.parent.name} > {tag.name}"
                hierarchy_patterns.append(pattern)
        
        hierarchy_counts = Counter(hierarchy_patterns).most_common(20)
        
        # Analysis of CSS classes
        classes = []
        for tag in soup.find_all(True):
            if 'class' in tag.attrs:
                for cls in tag.attrs['class']:
                    classes.append(f"{tag.name}.{cls}")
        
        class_counts = Counter(classes).most_common(20)
        
        # Create a signature dictionary
        signature_details = {
            "tag_counts": dict(tag_counts.most_common(20)),  # Top 20 tags
            "structure_sample": structure[:10],  # First 10 elements
            "structural_elements": {
                "has_tables": has_tables,
                "has_lists": has_lists,
                "has_sections": has_sections
            },
            "hierarchy_patterns": [(p, c) for p, c in hierarchy_counts],
            "class_patterns": [(c, cnt) for c, cnt in class_counts]
        }
        
        # Create a hash of the signature for quick comparison
        # We use a subset of the signature for the hash to make it more stable
        hash_components = {
            "tag_counts": dict(tag_counts.most_common(10)),
            "structure_sample": structure[:5],
            "hierarchy_patterns": [(p, c) for p, c in hierarchy_counts[:10]]
        }
        
        signature_str = json.dumps(hash_components, sort_keys=True)
        signature_hash = hashlib.md5(signature_str.encode()).hexdigest()
        
        return signature_hash, signature_details
    
    except Exception as e:
        logger.error(f"Error extracting schema signature: {e}")
        return None, None

def extract_document_type(html_content: str, metadata: Optional[Dict] = None) -> str:
    """
    Identify the document type based on content and metadata.
    
    Args:
        html_content: HTML content of the document
        metadata: Optional metadata dictionary
        
    Returns:
        str: Identified document type
    """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        text = soup.get_text().lower()
        
        # Look for type indicators in the document
        indicators = {
            "statute": r'\bstatute|\blaw|\bcode\b|\bsection\b|\b§\b',
            "regulation": r'\bregulation|\brule|\bcode of regulations\b|\badministrative code\b',
            "court_case": r'\bv\.\s|\bplaintiff|\bdefendant|\bappellant|\bcourt of appeals\b|\bsupreme court\b',
            "constitution": r'\bconstitution\b|\barticle \w+\b|\bamendment\b|\bpreamble\b',
            "ordinance": r'\bordinance\b|\bmunicipal code\b|\bcity code\b|\bcounty code\b',
            "executive_order": r'\bexecutive order\b|\bproclamation\b|\bby the authority\b',
            "policy": r'\bpolicy\b|\bguideline\b|\bprocedure\b|\bdirective\b'
        }
        
        # Check title first (if available)
        title_element = soup.find(['title', 'h1', 'h2'])
        if title_element:
            title_text = title_element.get_text().lower()
            for doc_type, pattern in indicators.items():
                if re.search(pattern, title_text):
                    return doc_type
        
        # Then check the whole text
        for doc_type, pattern in indicators.items():
            if re.search(pattern, text):
                return doc_type
        
        # Use metadata as fallback
        if metadata and 'document_type' in metadata and metadata['document_type']:
            return metadata['document_type']
        
        return "unknown"
    
    except Exception as e:
        logger.error(f"Error extracting document type: {e}")
        return "unknown"

def get_sample_elements(html_content: str, max_elements: int = 10) -> List[Dict]:
    """
    Extract sample elements from an HTML document to represent its structure.
    
    Args:
        html_content: HTML content of the document
        max_elements: Maximum number of elements to extract
        
    Returns:
        List[Dict]: List of dictionaries with element information
    """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        samples = []
        
        # Extract some representative elements
        for i, tag in enumerate(soup.find_all(True)):
            if i >= max_elements:
                break
                
            # Skip script, style, meta tags
            if tag.name in ['script', 'style', 'meta', 'link', 'head']:
                continue
                
            # Get tag attributes
            attrs = {}
            for key, value in tag.attrs.items():
                if isinstance(value, list):
                    attrs[key] = ' '.join(value)
                else:
                    attrs[key] = value
            
            # Get the element's text content (truncated)
            text = tag.get_text(strip=True)
            if len(text) > 100:
                text = text[:100] + "..."
            
            samples.append({
                "tag": tag.name,
                "attributes": attrs,
                "parent": tag.parent.name if tag.parent else None,
                "text_sample": text
            })
        
        return samples
    
    except Exception as e:
        logger.error(f"Error getting sample elements: {e}")
        return []

def compare_schemas(schema1: Dict, schema2: Dict) -> float:
    """
    Compare two schema signatures and return a similarity score.
    
    Args:
        schema1: First schema details dictionary
        schema2: Second schema details dictionary
        
    Returns:
        float: Similarity score (0.0 to 1.0)
    """
    try:
        # Compare tag counts
        tags1 = set(schema1.get('tag_counts', {}).keys())
        tags2 = set(schema2.get('tag_counts', {}).keys())
        tag_overlap = len(tags1.intersection(tags2)) / max(len(tags1), len(tags2)) if tags1 or tags2 else 0
        
        # Compare structural elements
        struct1 = schema1.get('structural_elements', {})
        struct2 = schema2.get('structural_elements', {})
        
        struct_keys = set(struct1.keys()).union(set(struct2.keys()))
        matching_structs = sum(1 for k in struct_keys if struct1.get(k) == struct2.get(k))
        struct_similarity = matching_structs / len(struct_keys) if struct_keys else 0
        
        # Compare hierarchy patterns
        hier1 = dict(schema1.get('hierarchy_patterns', []))
        hier2 = dict(schema2.get('hierarchy_patterns', []))
        
        hier_keys = set(hier1.keys()).intersection(set(hier2.keys()))
        hier_similarity = len(hier_keys) / max(len(hier1), len(hier2)) if hier1 or hier2 else 0
        
        # Weighted combination
        similarity = (tag_overlap * 0.4) + (struct_similarity * 0.2) + (hier_similarity * 0.4)
        return similarity
    
    except Exception as e:
        logger.error(f"Error comparing schemas: {e}")
        return 0.0 