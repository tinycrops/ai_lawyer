#!/usr/bin/env python
"""
Schema Manager for American Law Dataset processing.
Identifies, categorizes, and manages document schemas to improve parsing accuracy.
"""

import os
import json
import hashlib
from collections import defaultdict, Counter
from bs4 import BeautifulSoup
import sqlite3
from pathlib import Path
import re
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='schema_manager.log'
)
logger = logging.getLogger('schema_manager')

# Constants
SCHEMA_CACHE_DIR = "schema_cache"
SCHEMA_REGISTRY_FILE = os.path.join(SCHEMA_CACHE_DIR, "schema_registry.json")
DB_PATH = "american_law_data.db"
SAMPLE_LIMIT = 5  # Number of sample documents to save per schema

# Ensure cache directory exists
os.makedirs(SCHEMA_CACHE_DIR, exist_ok=True)

class SchemaManager:
    """Manages document schemas for the American Law Dataset"""
    
    def __init__(self):
        """Initialize the schema manager"""
        self.schema_registry = self._load_schema_registry()
        self.db_conn = self._get_db_connection()
        
    def _get_db_connection(self):
        """Return a connection to the SQLite database"""
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    
    def _load_schema_registry(self):
        """Load the schema registry from file or create a new one"""
        if os.path.exists(SCHEMA_REGISTRY_FILE):
            with open(SCHEMA_REGISTRY_FILE, 'r') as f:
                return json.load(f)
        return {
            "schemas": {},
            "document_types": {},
            "jurisdiction_schemas": defaultdict(list),
            "metadata": {
                "total_documents_analyzed": 0,
                "last_updated": None
            }
        }
    
    def save_schema_registry(self):
        """Save the schema registry to file"""
        # Convert defaultdict to regular dict for JSON serialization
        registry_copy = self.schema_registry.copy()
        registry_copy["jurisdiction_schemas"] = dict(registry_copy["jurisdiction_schemas"])
        
        with open(SCHEMA_REGISTRY_FILE, 'w') as f:
            json.dump(registry_copy, f, indent=2)
        
        logger.info(f"Schema registry saved with {len(self.schema_registry['schemas'])} schemas")
    
    def extract_schema_signature(self, html_content):
        """
        Extract a schema signature from HTML content
        Returns a tuple of (signature_hash, signature_details)
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Count tag frequencies
            tag_counts = Counter([tag.name for tag in soup.find_all()])
            
            # Extract basic structure (first 10 elements)
            structure = [tag.name for tag in list(soup.descendants)[:50] if hasattr(tag, 'name') and tag.name]
            
            # Check for specific structural elements
            has_tables = len(soup.find_all('table')) > 0
            has_lists = len(soup.find_all(['ul', 'ol'])) > 0
            has_sections = bool(re.search(r'section|§|\bsec\b|\bart\b|article', soup.get_text().lower()))
            
            # Create a signature dictionary
            signature = {
                "tag_counts": dict(tag_counts.most_common(20)),  # Top 20 tags
                "structure_sample": structure[:10],  # First 10 elements
                "structural_elements": {
                    "has_tables": has_tables,
                    "has_lists": has_lists,
                    "has_sections": has_sections
                }
            }
            
            # Create a hash of the signature for quick comparison
            signature_str = json.dumps(signature, sort_keys=True)
            signature_hash = hashlib.md5(signature_str.encode()).hexdigest()
            
            return signature_hash, signature
            
        except Exception as e:
            logger.error(f"Error extracting schema signature: {str(e)}")
            return None, None
    
    def extract_document_type(self, html_content, metadata=None):
        """Identify the document type based on content and metadata"""
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
            logger.error(f"Error extracting document type: {str(e)}")
            return "unknown"
    
    def analyze_document(self, document_id, html_content, metadata=None):
        """
        Analyze a document and update the schema registry
        Returns the schema hash
        """
        schema_hash, schema_details = self.extract_schema_signature(html_content)
        if not schema_hash:
            return None
            
        document_type = self.extract_document_type(html_content, metadata)
        
        # Update schema registry
        if schema_hash not in self.schema_registry["schemas"]:
            # New schema found
            self.schema_registry["schemas"][schema_hash] = {
                "details": schema_details,
                "document_count": 1,
                "document_types": [document_type],
                "sample_document_ids": [document_id],
                "processing_strategy": "default"  # Default strategy, can be updated manually
            }
        else:
            # Update existing schema
            schema = self.schema_registry["schemas"][schema_hash]
            schema["document_count"] += 1
            
            if document_type not in schema["document_types"]:
                schema["document_types"].append(document_type)
                
            if document_id not in schema["sample_document_ids"] and len(schema["sample_document_ids"]) < SAMPLE_LIMIT:
                schema["sample_document_ids"].append(document_id)
        
        # Update document type stats
        if document_type not in self.schema_registry["document_types"]:
            self.schema_registry["document_types"][document_type] = {
                "count": 1,
                "schemas": [schema_hash]
            }
        else:
            self.schema_registry["document_types"][document_type]["count"] += 1
            if schema_hash not in self.schema_registry["document_types"][document_type]["schemas"]:
                self.schema_registry["document_types"][document_type]["schemas"].append(schema_hash)
        
        # Update jurisdiction schemas if metadata has jurisdiction info
        if metadata and 'jurisdiction' in metadata:
            jurisdiction = metadata['jurisdiction']
            if schema_hash not in self.schema_registry["jurisdiction_schemas"][jurisdiction]:
                self.schema_registry["jurisdiction_schemas"][jurisdiction].append(schema_hash)
        
        # Update metadata
        self.schema_registry["metadata"]["total_documents_analyzed"] += 1
        
        return schema_hash
    
    def get_schema_prompt(self, schema_hash, document_type, jurisdiction=None):
        """Generate a prompt for parsing a document with this schema"""
        if schema_hash not in self.schema_registry["schemas"]:
            # Default generic prompt
            return self._get_default_prompt(document_type, jurisdiction)
        
        schema = self.schema_registry["schemas"][schema_hash]
        
        # If schema has a custom prompt template, use it
        if "prompt_template" in schema:
            prompt = schema["prompt_template"]
            # Replace placeholders
            prompt = prompt.replace("{document_type}", document_type)
            if jurisdiction:
                prompt = prompt.replace("{jurisdiction}", jurisdiction)
            return prompt
            
        # Otherwise use the default prompt with schema details
        prompt = self._get_default_prompt(document_type, jurisdiction)
        
        # Add schema-specific info
        structure_info = f"This document has a structure with: "
        
        if schema["details"]["structural_elements"]["has_tables"]:
            structure_info += "tables, "
        if schema["details"]["structural_elements"]["has_lists"]:
            structure_info += "lists, "
        if schema["details"]["structural_elements"]["has_sections"]:
            structure_info += "sections, "
            
        prompt += f"\n\n{structure_info.rstrip(', ')}."
        
        # Add processing strategy if available
        if "processing_notes" in schema:
            prompt += f"\n\n{schema['processing_notes']}"
            
        return prompt
    
    def _get_default_prompt(self, document_type, jurisdiction=None):
        """Generate a default prompt based on document type"""
        base_prompt = f"Parse this {document_type} document and extract its structured content."
        
        if jurisdiction:
            base_prompt += f" This document is from the jurisdiction of {jurisdiction}."
            
        type_specific = {
            "statute": "Extract section numbers, section titles, and the text of each section. Identify definitions, penalties, and effective dates if present.",
            "regulation": "Extract rule numbers, rule titles, and the text of each rule. Identify definitions, enforcement provisions, and effective dates if present.",
            "court_case": "Extract the case name, court, date, judges, parties, legal issues, holdings, and reasoning.",
            "constitution": "Extract article numbers, section numbers, and the text of each section. Identify amendments and their content if present.",
            "ordinance": "Extract ordinance numbers, titles, and the text of each ordinance. Identify definitions, penalties, and effective dates if present.",
            "executive_order": "Extract the order number, title, authority, and text of the order. Identify effective dates and signatories if present.",
            "policy": "Extract policy name, purpose, scope, and detailed policy provisions."
        }
        
        if document_type in type_specific:
            base_prompt += f" {type_specific[document_type]}"
            
        base_prompt += " Format the output as JSON with appropriate nested structure."
        
        return base_prompt
    
    def batch_analyze_documents(self, limit=100):
        """
        Analyze a batch of unprocessed documents
        Returns the number of documents analyzed
        """
        # Get unprocessed documents
        cursor = self.db_conn.execute(
            """
            SELECT id, document_id, html_path, metadata_path
            FROM jurisdiction_documents
            WHERE schema_hash IS NULL
            LIMIT ?
            """,
            (limit,)
        )
        
        records = cursor.fetchall()
        if not records:
            logger.info("No unprocessed documents found")
            return 0
            
        count = 0
        for record in records:
            try:
                # Load HTML content
                html_path = record['html_path']
                if not os.path.exists(html_path):
                    logger.warning(f"HTML file not found: {html_path}")
                    continue
                    
                with open(html_path, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                
                # Load metadata if available
                metadata = None
                if record['metadata_path'] and os.path.exists(record['metadata_path']):
                    with open(record['metadata_path'], 'r', encoding='utf-8') as f:
                        metadata = json.load(f)
                
                # Analyze document
                schema_hash = self.analyze_document(record['document_id'], html_content, metadata)
                if schema_hash:
                    # Update database
                    self.db_conn.execute(
                        """
                        UPDATE jurisdiction_documents
                        SET schema_hash = ?
                        WHERE id = ?
                        """,
                        (schema_hash, record['id'])
                    )
                    self.db_conn.commit()
                    count += 1
                    
                    if count % 10 == 0:
                        logger.info(f"Analyzed {count} documents")
                        
            except Exception as e:
                logger.error(f"Error analyzing document {record['document_id']}: {str(e)}")
                continue
                
        # Save schema registry
        self.save_schema_registry()
        
        logger.info(f"Completed batch analysis: {count} documents analyzed")
        return count
    
    def get_schema_statistics(self):
        """Get statistics about schemas in the registry"""
        stats = {
            "total_schemas": len(self.schema_registry["schemas"]),
            "total_documents_analyzed": self.schema_registry["metadata"]["total_documents_analyzed"],
            "document_types": {
                doc_type: data["count"] for doc_type, data in self.schema_registry["document_types"].items()
            },
            "top_schemas": [],
            "jurisdictions": {
                jurisdiction: len(schemas) for jurisdiction, schemas in self.schema_registry["jurisdiction_schemas"].items()
            }
        }
        
        # Get top schemas by document count
        schemas = [(schema_hash, data["document_count"]) for schema_hash, data in self.schema_registry["schemas"].items()]
        schemas.sort(key=lambda x: x[1], reverse=True)
        
        for schema_hash, count in schemas[:10]:  # Top 10 schemas
            schema = self.schema_registry["schemas"][schema_hash]
            stats["top_schemas"].append({
                "hash": schema_hash,
                "document_count": count,
                "document_types": schema["document_types"],
                "has_custom_strategy": "processing_strategy" in schema and schema["processing_strategy"] != "default"
            })
            
        return stats
    
    def update_schema_processing_strategy(self, schema_hash, strategy, prompt_template=None, processing_notes=None):
        """Update the processing strategy for a schema"""
        if schema_hash not in self.schema_registry["schemas"]:
            logger.error(f"Schema hash {schema_hash} not found in registry")
            return False
            
        schema = self.schema_registry["schemas"][schema_hash]
        schema["processing_strategy"] = strategy
        
        if prompt_template:
            schema["prompt_template"] = prompt_template
            
        if processing_notes:
            schema["processing_notes"] = processing_notes
            
        self.save_schema_registry()
        logger.info(f"Updated processing strategy for schema {schema_hash}")
        return True
    
    def export_schema_samples(self, output_dir=None):
        """Export sample documents for each schema to the specified directory"""
        if not output_dir:
            output_dir = os.path.join(SCHEMA_CACHE_DIR, "samples")
            
        os.makedirs(output_dir, exist_ok=True)
        
        for schema_hash, schema in self.schema_registry["schemas"].items():
            schema_dir = os.path.join(output_dir, schema_hash)
            os.makedirs(schema_dir, exist_ok=True)
            
            # Save schema details
            with open(os.path.join(schema_dir, "schema_details.json"), 'w') as f:
                json.dump(schema, f, indent=2)
            
            # Save sample documents
            for doc_id in schema["sample_document_ids"]:
                cursor = self.db_conn.execute(
                    """
                    SELECT html_path, metadata_path
                    FROM jurisdiction_documents
                    WHERE document_id = ?
                    """,
                    (doc_id,)
                )
                
                record = cursor.fetchone()
                if not record or not record['html_path'] or not os.path.exists(record['html_path']):
                    continue
                    
                # Copy HTML file
                try:
                    with open(record['html_path'], 'r', encoding='utf-8') as src:
                        with open(os.path.join(schema_dir, f"{doc_id}.html"), 'w', encoding='utf-8') as dst:
                            dst.write(src.read())
                            
                    # Copy metadata if available
                    if record['metadata_path'] and os.path.exists(record['metadata_path']):
                        with open(record['metadata_path'], 'r', encoding='utf-8') as src:
                            with open(os.path.join(schema_dir, f"{doc_id}_metadata.json"), 'w', encoding='utf-8') as dst:
                                dst.write(src.read())
                except Exception as e:
                    logger.error(f"Error exporting sample document {doc_id}: {str(e)}")
                    continue
        
        logger.info(f"Exported schema samples to {output_dir}")
        return output_dir

def main():
    """Main function for schema manager"""
    schema_manager = SchemaManager()
    
    # Analyze a batch of documents
    num_analyzed = schema_manager.batch_analyze_documents(limit=50)
    print(f"Analyzed {num_analyzed} documents")
    
    # Get schema statistics
    stats = schema_manager.get_schema_statistics()
    print(f"Total schemas: {stats['total_schemas']}")
    print(f"Total documents analyzed: {stats['total_documents_analyzed']}")
    print(f"Document types: {stats['document_types']}")
    print("\nTop schemas:")
    for schema in stats['top_schemas']:
        print(f"  {schema['hash']}: {schema['document_count']} documents, types: {schema['document_types']}")
    
    # Export schema samples
    samples_dir = schema_manager.export_schema_samples()
    print(f"Exported schema samples to {samples_dir}")

if __name__ == "__main__":
    main() 