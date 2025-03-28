"""
Gemini API interface for generating parsing methodologies.
"""

import os
import re
import json
import logging
from typing import Dict, Optional, Any, List, Tuple

import google.generativeai as genai

from tinycrops_lawyer.config import DEFAULT_MODEL, DEFAULT_TEMPERATURE, DEFAULT_MAX_OUTPUT_TOKENS, get_gemini_api_key

# Initialize logger
logger = logging.getLogger(__name__)

# Initialize Gemini
API_KEY = get_gemini_api_key()
if API_KEY:
    genai.configure(api_key=API_KEY)
else:
    logger.warning("No Gemini API key found. API functionality will not work.")

def initialize_model(model_name: str = DEFAULT_MODEL):
    """
    Initialize the Gemini model with specified configuration.
    
    Args:
        model_name: Name of the Gemini model to use
        
    Returns:
        GenerativeModel: Configured Gemini model
    """
    try:
        generation_config = {
            "temperature": DEFAULT_TEMPERATURE,
            "top_p": 0.95,
            "top_k": 40,
            "max_output_tokens": DEFAULT_MAX_OUTPUT_TOKENS,
        }
        
        model = genai.GenerativeModel(
            model_name=model_name,
            generation_config=generation_config
        )
        
        return model
    except Exception as e:
        logger.error(f"Error initializing Gemini model: {e}")
        return None

def generate_parsing_methodology(schema_signature: Dict, sample_html: str, model_name: str = DEFAULT_MODEL) -> Optional[str]:
    """
    Generate a Python methodology for parsing HTML documents matching a schema.
    
    Args:
        schema_signature: Schema signature details
        sample_html: Sample HTML content for the schema
        model_name: Name of the Gemini model to use
        
    Returns:
        Optional[str]: Python code string with the parsing methodology or None if failed
    """
    if not API_KEY:
        logger.error("No Gemini API key configured. Cannot generate parsing methodology.")
        return None
    
    try:
        model = initialize_model(model_name)
        if not model:
            logger.error("Failed to initialize Gemini model")
            return None
        
        # Prepare schema signature details for the prompt
        signature_details = json.dumps(schema_signature, indent=2)
        
        # Truncate sample HTML to avoid token limits (keep first ~10000 chars)
        truncated_html = sample_html[:10000]
        if len(sample_html) > 10000:
            truncated_html += "\n... [truncated] ...\n"
            # Add some content from the end too
            truncated_html += sample_html[-2000:]
        
        # Create the prompt
        prompt = f"""
You are an expert Python developer specializing in HTML parsing with libraries like BeautifulSoup.
Analyze the following HTML structure (represented by its signature and a sample) from an American legal document.
Your task is to generate a SINGLE Python function named 'parse_html' that accepts one argument: 'html_string' (a string containing the raw HTML content).
This function should parse the 'html_string' using BeautifulSoup or similar standard Python libraries (do not use external libraries that aren't typically available like pandas within the function itself).
The function MUST return a Python dictionary (JSON-serializable) containing the extracted structured data.

The output dictionary should include:
- document_id: A placeholder where the document ID will be inserted (use "unknown" as default)
- jurisdiction: Extracted jurisdiction information (state/city/county) if available
- document_type: The type of document (statute, ordinance, regulation, etc.)
- sections: An array of extracted sections, with each section containing:
  - section_id: A unique identifier for the section
  - section_num: The section number if available
  - section_title: The section heading/title if available
  - section_text: The main text content of the section
  - section_refs: Any references to other sections or documents (optional)

Focus on proper extraction of the legal content structure. Ensure your code handles:
1. Proper section identification and nesting
2. Accurate extraction of section numbers and titles
3. Clean extraction of section content without markup
4. Robust error handling for variations in the document structure

Schema Signature Details:
{signature_details}

Sample HTML:
```html
{truncated_html}
```

Provide ONLY the Python function code, enclosed in triple backticks (python ...). Do not include explanations outside the code block or example usage.
The function should be self-contained, properly handle edge cases, and be production-ready.
"""
        
        # Call Gemini API
        response = model.generate_content(prompt)
        response_text = response.text
        
        # Extract code block from response
        code_match = re.search(r'```python(.*?)```', response_text, re.DOTALL)
        if code_match:
            code = code_match.group(1).strip()
        else:
            # Try without language specifier
            code_match = re.search(r'```(.*?)```', response_text, re.DOTALL)
            if code_match:
                code = code_match.group(1).strip()
            else:
                # Use the entire response if no code blocks found
                code = response_text.strip()
        
        # Validate that the code contains a parse_html function
        if 'def parse_html(' not in code:
            logger.error("Generated code does not contain parse_html function")
            return None
        
        return code
    
    except Exception as e:
        logger.error(f"Error generating parsing methodology: {e}")
        return None

def test_parsing_methodology(code: str, html_content: str) -> Tuple[bool, Optional[Dict], Optional[str]]:
    """
    Test a parsing methodology on an HTML document.
    
    Args:
        code: Python code string with parse_html function
        html_content: HTML content to parse
        
    Returns:
        Tuple[bool, Optional[Dict], Optional[str]]: A tuple containing:
            - success: True if parsing succeeded, False otherwise
            - result: The parsed dictionary if successful, None otherwise
            - error: Error message if failed, None otherwise
    """
    try:
        # Create a local scope
        local_scope = {}
        
        # Execute the code in the local scope
        exec(code, globals(), local_scope)
        
        # Ensure parse_html function exists
        if 'parse_html' not in local_scope:
            return False, None, "parse_html function not found in code"
        
        # Call the function
        result = local_scope['parse_html'](html_content)
        
        # Validate result
        if not isinstance(result, dict):
            return False, None, f"Result is not a dictionary (got {type(result).__name__})"
        
        # Ensure it can be JSON serialized
        try:
            json.dumps(result)
        except TypeError as e:
            return False, None, f"Result is not JSON serializable: {e}"
        
        return True, result, None
    
    except Exception as e:
        return False, None, f"Error executing methodology: {e}"

def parse_document_with_gemini(html_content: str, document_info: Dict, model_name: str = DEFAULT_MODEL) -> Optional[Dict]:
    """
    Parse an HTML document directly with Gemini.
    This is used as a fallback if no methodology exists or can be generated.
    
    Args:
        html_content: HTML content to parse
        document_info: Document metadata dictionary
        model_name: Name of the Gemini model to use
        
    Returns:
        Optional[Dict]: Parsed document structure or None if failed
    """
    if not API_KEY:
        logger.error("No Gemini API key configured. Cannot parse document.")
        return None
    
    try:
        model = initialize_model(model_name)
        if not model:
            logger.error("Failed to initialize Gemini model")
            return None
        
        # Truncate HTML content
        truncated_html = html_content[:50000]  # Limit to 50k chars
        
        # Create the prompt
        prompt = f"""
You are a legal document parser specializing in American municipal law.

DOCUMENT INFORMATION:
- Document ID: {document_info.get('document_id', 'Unknown')}
- Document Type: {document_info.get('document_type', 'Unknown')}
- Jurisdiction: {document_info.get('place_name', 'Unknown')}, {document_info.get('state_name', 'Unknown')}
- Date: {document_info.get('document_date', 'Unknown')}

HTML CONTENT:
{truncated_html}

PARSING INSTRUCTIONS:
1. Extract the main document content, preserving section structure
2. Identify section headings, numbers, and content
3. Format date references consistently as YYYY-MM-DD
4. Extract any legislative history or citation information
5. Preserve footnotes and references

OUTPUT FORMAT:
Return a JSON object with the following fields:
- document_id: Unique identifier from the document info
- jurisdiction: Place and state information
- document_type: The type of document
- sections: Array of section objects containing:
  - section_id: Unique section identifier (use document_id + "_sec_" + index)
  - section_num: The section number
  - section_title: The section heading
  - section_text: The main text content
  - section_refs: Any references to other sections or documents (optional)
"""
        
        # Call Gemini API
        response = model.generate_content(prompt)
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
            return parsed_data
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON response: {e}")
            logger.error(f"Response text: {response_text[:500]}...")
            return None
    
    except Exception as e:
        logger.error(f"Error parsing document with Gemini: {e}")
        return None 