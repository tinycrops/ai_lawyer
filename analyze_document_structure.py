#!/usr/bin/env python
"""
Script to analyze the HTML structure of a sample document from the American Law dataset
to better understand how to extract sections and other structured data.
"""
import os
import sys
import json
import sqlite3
from pathlib import Path
from bs4 import BeautifulSoup
from pprint import pprint
from collections import Counter

# Constants
DB_PATH = "american_law_data.db"
OUTPUT_DIR = "analysis_results"

# Create output directory
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_sample_documents(limit=5):
    """
    Get sample documents from the database
    
    Args:
        limit: Maximum number of documents to retrieve
        
    Returns:
        List of document dictionaries
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Query documents
    cursor.execute("SELECT * FROM documents WHERE raw_html IS NOT NULL LIMIT ?", (limit,))
    rows = cursor.fetchall()
    
    # Convert to dictionaries
    documents = []
    for row in rows:
        doc = dict(row)
        documents.append(doc)
    
    conn.close()
    return documents


def analyze_html_structure(html_content):
    """
    Analyze the HTML structure of a document
    
    Args:
        html_content: Raw HTML string
        
    Returns:
        Dictionary with analysis results
    """
    if not html_content:
        return {"error": "No HTML content provided"}
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Analyze tag structure
        tags = [tag.name for tag in soup.find_all()]
        tag_counts = Counter(tags)
        
        # Analyze class attributes
        classes = []
        for tag in soup.find_all(True, class_=True):
            if isinstance(tag['class'], list):
                classes.extend(tag['class'])
            else:
                classes.append(tag['class'])
        class_counts = Counter(classes)
        
        # Identify potential section elements
        potential_sections = []
        
        # Look for div/section elements with section-like classes
        section_classes = ['section', 'sec', 'part', 'chapter', 'article', 'division']
        for section_class in section_classes:
            elements = soup.find_all(['div', 'section'], class_=lambda c: c and section_class in c.lower())
            if elements:
                for elem in elements[:3]:  # Just show first 3 examples
                    potential_sections.append({
                        "tag": elem.name,
                        "classes": elem.get('class', []),
                        "id": elem.get('id', ''),
                        "text_preview": elem.get_text()[:100].strip()
                    })
        
        # Look for heading patterns
        headings = []
        for i, heading in enumerate(soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])):
            if i >= 10:  # Limit to first 10 headings
                break
            headings.append({
                "tag": heading.name,
                "text": heading.get_text().strip(),
                "classes": heading.get('class', [])
            })
        
        # Find parent-child patterns that might indicate sections
        patterns = []
        for tag in ['div', 'section', 'article']:
            for elem in soup.find_all(tag)[:10]:  # Limit to first 10
                children = [child.name for child in elem.find_all(recursive=False) if child.name]
                if children:
                    patterns.append({
                        "parent": {
                            "tag": elem.name,
                            "classes": elem.get('class', []),
                            "id": elem.get('id', '')
                        },
                        "children": children
                    })
        
        return {
            "tag_counts": dict(tag_counts.most_common(15)),
            "class_counts": dict(class_counts.most_common(15)),
            "potential_sections": potential_sections,
            "headings": headings,
            "parent_child_patterns": patterns[:5]  # Limit to first 5 patterns
        }
        
    except Exception as e:
        return {"error": str(e)}


def detect_section_pattern(html_content):
    """
    Try to detect the most likely pattern for sections in the document
    
    Args:
        html_content: Raw HTML string
        
    Returns:
        Dictionary with pattern detection results
    """
    if not html_content:
        return {"pattern": "unknown", "reason": "No HTML content provided"}
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Check for explicit section elements
        section_elements = soup.find_all(['div', 'section'], class_=['section', 'sec', 'statute-section'])
        if section_elements:
            return {
                "pattern": "explicit_sections",
                "reason": f"Found {len(section_elements)} explicit section elements",
                "selector": "div.section, section.section, div.sec, section.sec, div.statute-section",
                "count": len(section_elements)
            }
        
        # Check for heading-based structure
        headings = soup.find_all(['h1', 'h2', 'h3', 'h4'])
        if headings:
            # Check for section numbering in headings
            section_headings = [h for h in headings if 'section' in h.get_text().lower() or '§' in h.get_text()]
            if section_headings:
                return {
                    "pattern": "section_headings",
                    "reason": f"Found {len(section_headings)} headings with section indicators",
                    "selector": "h1, h2, h3, h4",
                    "count": len(section_headings)
                }
            else:
                return {
                    "pattern": "general_headings",
                    "reason": f"Found {len(headings)} headings that may indicate document structure",
                    "selector": "h1, h2, h3, h4",
                    "count": len(headings)
                }
        
        # Check for paragraph-based structure
        paragraphs = soup.find_all('p')
        if paragraphs:
            # Check if paragraphs have ids or classes that suggest structure
            structured_paras = [p for p in paragraphs if p.get('id') or p.get('class')]
            if structured_paras:
                return {
                    "pattern": "structured_paragraphs",
                    "reason": f"Found {len(structured_paras)} paragraphs with IDs or classes",
                    "selector": "p[id], p[class]",
                    "count": len(structured_paras)
                }
        
        # Check for list-based structure
        list_items = soup.find_all(['ol', 'ul', 'li'])
        if list_items:
            return {
                "pattern": "list_structure",
                "reason": f"Found {len(list_items)} list elements that may indicate structure",
                "selector": "ol, ul, li",
                "count": len(list_items)
            }
        
        return {
            "pattern": "unknown",
            "reason": "Could not detect a clear section pattern"
        }
        
    except Exception as e:
        return {"pattern": "error", "reason": str(e)}


def extract_sample_sections(html_content, pattern_info):
    """
    Extract sample sections based on the detected pattern
    
    Args:
        html_content: Raw HTML string
        pattern_info: Pattern detection results
        
    Returns:
        List of extracted sections
    """
    if not html_content:
        return []
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        sections = []
        
        pattern = pattern_info.get("pattern", "unknown")
        
        if pattern == "explicit_sections":
            # Use explicit section elements
            selector_str = pattern_info.get("selector", "div.section")
            selectors = selector_str.split(", ") if isinstance(selector_str, str) else ["div.section"]
            
            for selector in selectors:
                if "." in selector:
                    tag, cls = selector.split(".")
                    elements = soup.find_all(tag, class_=cls)
                else:
                    elements = soup.find_all(selector)
                
                for i, element in enumerate(elements[:5]):  # Limit to first 5
                    # Try to find heading
                    heading = element.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'strong'])
                    heading_text = heading.get_text().strip() if heading else ""
                    
                    # Get section content
                    content = element.get_text().strip()
                    if heading:
                        # Remove heading text from content
                        content = content.replace(heading_text, "", 1).strip()
                    
                    sections.append({
                        "section_id": f"section_{i+1}",
                        "heading": heading_text,
                        "content_preview": content[:200] + "..." if len(content) > 200 else content
                    })
        
        elif pattern in ["section_headings", "general_headings"]:
            # Use headings to define sections
            headings = soup.find_all(['h1', 'h2', 'h3', 'h4'])
            
            for i, heading in enumerate(headings[:5]):  # Limit to first 5
                heading_text = heading.get_text().strip()
                
                # Get content (all elements until next heading)
                content = []
                for sibling in heading.next_siblings:
                    if sibling.name in ['h1', 'h2', 'h3', 'h4']:
                        break
                    if hasattr(sibling, 'get_text'):
                        content.append(sibling.get_text().strip())
                
                content_text = " ".join(content)
                sections.append({
                    "section_id": f"section_{i+1}",
                    "heading": heading_text,
                    "content_preview": content_text[:200] + "..." if len(content_text) > 200 else content_text
                })
        
        elif pattern == "structured_paragraphs":
            # Use structured paragraphs
            paras = soup.find_all('p')
            structured_paras = [p for p in paras if p.get('id') or p.get('class')]
            
            for i, para in enumerate(structured_paras[:5]):  # Limit to first 5
                para_text = para.get_text().strip()
                sections.append({
                    "section_id": f"section_{i+1}",
                    "heading": f"Paragraph {i+1}",
                    "content_preview": para_text[:200] + "..." if len(para_text) > 200 else para_text
                })
                
        # If no sections found yet, try a class-based approach
        if not sections:
            # Look for common content classes
            content_classes = ['content', 'text', 'section-content', 'body']
            for cls in content_classes:
                elements = soup.find_all(class_=lambda c: c and cls in c.lower())
                if elements:
                    for i, elem in enumerate(elements[:5]):
                        content = elem.get_text().strip()
                        sections.append({
                            "section_id": f"content_{i+1}",
                            "heading": f"Content {i+1}",
                            "content_preview": content[:200] + "..." if len(content) > 200 else content
                        })
                    break
                    
        # If still no sections, try paragraph grouping
        if not sections:
            paragraphs = soup.find_all('p')
            if paragraphs:
                # Group paragraphs into pseudo-sections
                current_section = []
                all_sections = []
                
                for p in paragraphs:
                    text = p.get_text().strip()
                    if not text:
                        continue
                        
                    # Check if this might be a heading (shorter text, possibly bold/strong)
                    is_heading = len(text) < 100 and (p.find('strong') or p.find('b') or p.get('class'))
                    
                    if is_heading and current_section:
                        # Start a new section
                        all_sections.append(current_section)
                        current_section = [p]
                    else:
                        current_section.append(p)
                
                # Add the last section
                if current_section:
                    all_sections.append(current_section)
                
                # Convert grouped paragraphs to sections
                for i, section_paras in enumerate(all_sections[:5]):
                    if not section_paras:
                        continue
                        
                    # First paragraph might be a heading
                    heading = section_paras[0].get_text().strip() if len(section_paras) > 1 else ""
                    
                    # Rest is content
                    content_paras = section_paras[1:] if len(section_paras) > 1 else section_paras
                    content = " ".join(p.get_text().strip() for p in content_paras)
                    
                    sections.append({
                        "section_id": f"para_section_{i+1}",
                        "heading": heading,
                        "content_preview": content[:200] + "..." if len(content) > 200 else content
                    })
        
        return sections
        
    except Exception as e:
        print(f"Error extracting sections: {e}")
        import traceback
        traceback.print_exc()
        return []


def main():
    print("Analyzing document structure from the American Law dataset...")
    
    # Get sample documents
    sample_limit = 5
    print(f"Retrieving {sample_limit} sample documents...")
    documents = get_sample_documents(limit=sample_limit)
    
    if not documents:
        print("No documents found in the database. Make sure to run fetch_american_law_data.py first.")
        return
    
    print(f"Retrieved {len(documents)} documents for analysis\n")
    
    for i, doc in enumerate(documents):
        print(f"Analyzing document {i+1}/{len(documents)} - CID: {doc['cid']}")
        
        # Analyze HTML structure
        html_analysis = analyze_html_structure(doc['raw_html'])
        
        # Detect section pattern
        pattern_info = detect_section_pattern(doc['raw_html'])
        print(f"Detected pattern: {pattern_info['pattern']} - {pattern_info['reason']}")
        
        # Extract sample sections
        sections = extract_sample_sections(doc['raw_html'], pattern_info)
        print(f"Extracted {len(sections)} sample sections\n")
        
        # Save analysis results
        analysis_results = {
            "document_id": doc['doc_id'],
            "cid": doc['cid'],
            "place_name": doc.get('place_name', ''),
            "state_code": doc.get('state_code', ''),
            "html_structure": html_analysis,
            "section_pattern": pattern_info,
            "sample_sections": sections
        }
        
        # Save to file
        output_file = os.path.join(OUTPUT_DIR, f"doc_{doc['doc_id']}_analysis.json")
        with open(output_file, 'w') as f:
            json.dump(analysis_results, f, indent=2)
        
        print(f"Saved analysis results to {output_file}\n")
    
    print("Analysis complete. Check the results in the analysis_results directory.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc() 