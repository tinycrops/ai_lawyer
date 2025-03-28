#!/usr/bin/env python
"""
Schema-based parser for the American Law Dataset.
Uses Gemini to normalize HTML documents based on their schema patterns.
"""

import os
import json
import hashlib
from pathlib import Path
import re
from typing import Dict, List, Any, Optional
from bs4 import BeautifulSoup
import google.generativeai as genai
from tqdm import tqdm

# Constants
CACHE_DIR = "schema_cache"
LLM_MODEL = "gemini-2.0-flash"  # Gemini model to use

# Create cache directory
os.makedirs(CACHE_DIR, exist_ok=True)

class SchemaParser:
    """Parser that uses schema detection to guide LLM processing"""
    
    def __init__(self, model=None):
        """Initialize the schema parser"""
        self.model = model
        self.schema_cache = {}
        self.load_schema_cache()
        
    def load_schema_cache(self):
        """Load schema signatures from cache"""
        cache_file = os.path.join(CACHE_DIR, "schema_signatures.json")
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'r') as f:
                    self.schema_cache = json.load(f)
                print(f"Loaded {len(self.schema_cache)} schema signatures from cache")
            except Exception as e:
                print(f"Error loading schema cache: {e}")
    
    def save_schema_cache(self):
        """Save schema signatures to cache"""
        cache_file = os.path.join(CACHE_DIR, "schema_signatures.json")
        try:
            with open(cache_file, 'w') as f:
                json.dump(self.schema_cache, f, indent=2)
        except Exception as e:
            print(f"Error saving schema cache: {e}")
    
    def extract_schema_signature(self, html_content: str) -> Dict[str, Any]:
        """Extract a signature from HTML content that represents its schema structure"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Create a structure representation with tag hierarchy
        structure = []
        tag_hierarchy = []
        
        # Get all tags and their classes
        for tag in soup.find_all(True):
            # Get tag name
            tag_name = tag.name
            
            # Get classes
            classes = tag.get('class', [])
            class_str = '.'.join(sorted(classes)) if classes else ''
            
            # Combine tag and class info
            tag_info = f"{tag_name}[{class_str}]"
            structure.append(tag_info)
            
            # Track parent-child relationships for a hierarchy
            if tag.parent and tag.parent.name:
                parent_classes = tag.parent.get('class', [])
                parent_class_str = '.'.join(sorted(parent_classes)) if parent_classes else ''
                parent_info = f"{tag.parent.name}[{parent_class_str}]"
                hierarchy_info = f"{parent_info} > {tag_info}"
                tag_hierarchy.append(hierarchy_info)
        
        # Create a signature based on unique tag structures and their counts
        from collections import Counter
        counter = Counter(structure)
        signature_parts = [f"{tag}:{count}" for tag, count in sorted(counter.items())]
        signature = ";".join(signature_parts)
        
        # Create a hash of the signature for easier comparison
        signature_hash = hashlib.md5(signature.encode()).hexdigest()
        
        # Create a structured representation of the schema
        schema_info = {
            'signature_hash': signature_hash,
            'tag_count': len(structure),
            'unique_tag_count': len(counter),
            'hierarchy_patterns': Counter(tag_hierarchy).most_common(10)
        }
        
        # Cache the schema
        if signature_hash not in self.schema_cache:
            self.schema_cache[signature_hash] = {
                'signature': signature,
                'hierarchy_patterns': schema_info['hierarchy_patterns'],
                'document_count': 1
            }
        else:
            self.schema_cache[signature_hash]['document_count'] += 1
            
        # Save periodically
        if self.schema_cache[signature_hash]['document_count'] % 10 == 0:
            self.save_schema_cache()
            
        return schema_info
    
    def extract_document_type(self, html_content: str) -> str:
        """Extract document type from HTML content"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Possible indicators of document type
        doc_type = "Unknown"
        
        # Look for title elements
        title_elements = soup.find_all(['div', 'p', 'h1', 'h2', 'h3'], 
                                      class_=['chunk-title', 'bc', 'h0', 'title'])
        
        for element in title_elements:
            text = element.get_text().strip().lower()
            
            # Check for common document types in the title
            for dtype in ['ordinance', 'charter', 'code', 'statute', 'regulation', 
                         'footnote', 'resolution']:
                if dtype in text:
                    return dtype.title()
        
        # Check for tables which might indicate reference material
        if soup.find('table'):
            table_headers = [th.get_text().strip().lower() for th in soup.find_all('th')]
            if table_headers:
                doc_type = 'Reference'
        
        # Check for footnotes
        if soup.find(class_='footnote-content') or soup.find(class_='footnote'):
            doc_type = 'Footnote'
        
        # Look for section indicators
        section_patterns = [r'section \d+', r'§ \d+', r'sec\. \d+']
        text = soup.get_text().lower()
        for pattern in section_patterns:
            if re.search(pattern, text):
                doc_type = 'Code'
                break
        
        return doc_type
    
    def generate_schema_prompt(self, schema_info: Dict, document_info: Dict, html_content: str) -> str:
        """Generate a schema-aware prompt for the LLM"""
        # Get schema signature info
        signature_hash = schema_info['signature_hash']
        schema_desc = "Unknown schema"
        
        if signature_hash in self.schema_cache:
            # Get top hierarchy patterns
            hierarchy_patterns = self.schema_cache[signature_hash]['hierarchy_patterns']
            hierarchy_desc = "\n".join([f"- {pattern[0]}: {pattern[1]} occurrences" 
                                      for pattern in hierarchy_patterns[:5]])
            schema_desc = f"Schema with {schema_info['unique_tag_count']} unique tags.\nCommon patterns:\n{hierarchy_desc}"
        
        # Get document type
        doc_type = document_info.get('document_type') or self.extract_document_type(html_content)
        
        # Create prompt
        prompt = f"""
You are a legal document parser specializing in American municipal law.

DOCUMENT SCHEMA:
{schema_desc}

DOCUMENT TYPE:
{doc_type}

SOURCE JURISDICTION:
{document_info.get('place_name', 'Unknown')}, {document_info.get('state_name', 'Unknown')}

HTML CONTENT:
{html_content[:50000]}  # Truncate if too long

PARSING INSTRUCTIONS:
1. Extract the main document content, preserving section structure
2. Identify section headings, numbers, and content
3. Format date references consistently as YYYY-MM-DD
4. Extract any legislative history or citation information
5. Preserve footnotes and references

OUTPUT FORMAT:
Return a JSON object with the following fields:
- document_id: "{document_info.get('doc_id', 'unknown')}"
- jurisdiction: Place and state information
- document_type: The type of document
- sections: Array of section objects containing:
  - section_id: Unique section identifier (use document_id + "_sec_" + index)
  - section_num: The section number or identifier
  - section_title: The section heading
  - section_text: The main text content
  - section_refs: Any references to other sections or documents (optional)
"""
        return prompt
    
    def parse_document(self, document_info: Dict, html_content: str) -> Dict:
        """Parse an HTML document using schema detection and LLM"""
        if not self.model:
            print("No LLM model available for processing.")
            return None
        
        # Extract schema signature
        schema_info = self.extract_schema_signature(html_content)
        
        # Generate schema-aware prompt
        prompt = self.generate_schema_prompt(schema_info, document_info, html_content)
        
        try:
            # Generate response from LLM
            response = self.model.generate_content(prompt)
            response_text = response.text
            
            # Try to extract JSON from the response
            try:
                # Find JSON object in the response if surrounded by backticks
                if "```json" in response_text:
                    json_text = response_text.split("```json")[1].split("```")[0].strip()
                elif "```" in response_text:
                    json_text = response_text.split("```")[1].split("```")[0].strip()
                else:
                    json_text = response_text
                    
                parsed_data = json.loads(json_text)
                
                # Add schema signature to the parsed data
                parsed_data['schema_signature'] = schema_info['signature_hash']
                
                return parsed_data
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON response: {e}")
                print(f"Response text: {response_text[:500]}...")
                return None
                
        except Exception as e:
            print(f"Error processing with LLM: {e}")
            return None
    
    def process_html_batch(self, documents: List[Dict], html_contents: List[str]) -> List[Dict]:
        """Process a batch of HTML documents"""
        results = []
        
        for i, (document, html) in enumerate(zip(documents, html_contents)):
            print(f"Processing document {i+1}/{len(documents)}: {document.get('doc_id')}")
            parsed_data = self.parse_document(document, html)
            
            if parsed_data:
                results.append(parsed_data)
            else:
                print(f"Failed to parse document {document.get('doc_id')}")
        
        return results

def setup_gemini():
    """Configure the Gemini API"""
    try:
        # Look for API key in environment variable or .env file
        from dotenv import load_dotenv
        load_dotenv()
        
        api_key = os.environ.get("GOOGLE_API_KEY")
        if not api_key:
            print("Warning: No Google API key found. LLM processing will not work.")
            return None
        
        genai.configure(api_key=api_key)
        
        # Configure the model
        generation_config = {
            "temperature": 0.2,
            "top_p": 0.95,
            "top_k": 40,
            "max_output_tokens": 8192,
        }
        
        model = genai.GenerativeModel(
            model_name=LLM_MODEL,
            generation_config=generation_config
        )
        
        return model
    except Exception as e:
        print(f"Error setting up Gemini: {e}")
        return None

def main():
    """Test the schema parser"""
    # Set up Gemini model
    model = setup_gemini()
    if not model:
        print("Failed to set up Gemini model. Exiting.")
        return
    
    # Create schema parser
    parser = SchemaParser(model)
    
    # Load a sample document from file
    sample_file = "data_cache/sample_document.html"
    if os.path.exists(sample_file):
        with open(sample_file, 'r') as f:
            html_content = f.read()
            
        # Create a sample document info
        doc_info = {
            'doc_id': 'sample_doc_001',
            'place_name': 'Example City',
            'state_name': 'California',
            'document_type': 'Ordinance'
        }
        
        # Parse the document
        parsed_data = parser.parse_document(doc_info, html_content)
        
        if parsed_data:
            # Print the result
            print(json.dumps(parsed_data, indent=2))
            
            # Save the result
            output_file = "schema_cache/sample_parsed.json"
            with open(output_file, 'w') as f:
                json.dump(parsed_data, f, indent=2)
            
            print(f"Saved parsed document to {output_file}")
        else:
            print("Failed to parse sample document")
    else:
        print(f"Sample file not found: {sample_file}")
        print("Please add a sample HTML document to test the parser")

if __name__ == "__main__":
    main() 