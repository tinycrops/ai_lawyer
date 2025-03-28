import pandas as pd
import pyarrow.parquet as pq
from huggingface_hub import hf_hub_download, list_repo_files
import json
import re
from bs4 import BeautifulSoup
import hashlib
from collections import defaultdict, Counter
import os
import random

# Create cache directory for files
os.makedirs("cache", exist_ok=True)

def get_file_list():
    """Get all files in the dataset repository"""
    files = list_repo_files('the-ride-never-ends/american_law', repo_type='dataset')
    return files

def get_html_files(files):
    """Filter out only the HTML parquet files"""
    return [f for f in files if f.endswith('_html.parquet')]

def get_citation_files(files):
    """Filter out only the citation parquet files"""
    return [f for f in files if f.endswith('_citation.parquet')]

def get_metadata_files(files):
    """Filter out only the metadata JSON files"""
    return [f for f in files if f.startswith('american_law/metadata/') and f.endswith('.json')]

def download_and_read_parquet(filename):
    """Download and read a parquet file"""
    cache_path = f"cache/{os.path.basename(filename)}"
    
    if os.path.exists(cache_path):
        return pd.read_parquet(cache_path)
    
    file_path = hf_hub_download(
        repo_id='the-ride-never-ends/american_law',
        filename=filename,
        repo_type='dataset'
    )
    df = pd.read_parquet(file_path)
    
    # Cache the dataframe
    df.to_parquet(cache_path)
    
    return df

def download_and_read_json(filename):
    """Download and read a JSON file"""
    cache_path = f"cache/{os.path.basename(filename)}"
    
    if os.path.exists(cache_path):
        with open(cache_path, 'r') as f:
            return json.load(f)
    
    file_path = hf_hub_download(
        repo_id='the-ride-never-ends/american_law',
        filename=filename,
        repo_type='dataset'
    )
    
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    # Cache the data
    with open(cache_path, 'w') as f:
        json.dump(data, f)
    
    return data

def extract_html_structure(html_content):
    """Extract structure from HTML content and return a signature"""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Create a structure representation with tag hierarchy
    structure = []
    tag_hierarchy = []
    
    # Track element attributes
    element_attrs = defaultdict(list)
    
    for tag in soup.find_all(True):
        # Get tag name
        tag_name = tag.name
        
        # Get classes
        classes = tag.get('class', [])
        class_str = '.'.join(sorted(classes)) if classes else ''
        
        # Extract other useful attributes
        for attr in ['id', 'data-chunk-id', 'data-product-id', 'block_type']:
            if tag.has_attr(attr):
                element_attrs[attr].append(tag[attr])
        
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
    counter = Counter(structure)
    hierarchy_counter = Counter(tag_hierarchy)
    
    signature_parts = [f"{tag}:{count}" for tag, count in sorted(counter.items())]
    signature = ";".join(signature_parts)
    
    # Create a hash of the signature for easier comparison
    signature_hash = hashlib.md5(signature.encode()).hexdigest()
    
    return {
        'signature': signature,
        'signature_hash': signature_hash,
        'tag_count': len(structure),
        'unique_tag_count': len(counter),
        'hierarchy_patterns': dict(hierarchy_counter.most_common(10)),
        'attribute_patterns': {k: Counter(v).most_common(5) for k, v in element_attrs.items()}
    }

def analyze_citation_data(citation_file):
    """Analyze the structure of citation data"""
    df = download_and_read_parquet(citation_file)
    
    # Get column info
    columns = df.columns.tolist()
    
    # Sample a few rows to understand content
    sample_data = df.head(3)
    
    # Count non-null values for each column
    non_null_counts = df.count()
    
    # Identify columns that are always/mostly populated
    total_rows = len(df)
    column_presence = {col: (non_null_counts[col] / total_rows * 100) for col in columns}
    
    return {
        'columns': columns,
        'sample_data': sample_data.to_dict('records'),
        'non_null_percentage': column_presence
    }

def extract_document_type_from_html(html_content):
    """Try to extract document type from HTML content"""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Possible indicators of document type
    doc_type = None
    
    # Look for title elements
    title_elements = soup.find_all(['div', 'p'], class_=['chunk-title', 'bc', 'h0'])
    
    for element in title_elements:
        text = element.get_text().strip().lower()
        
        # Check for common document types in the title
        if any(doc_type in text for doc_type in ['ordinance', 'charter', 'code', 'statute', 'regulation', 'footnote']):
            for dtype in ['ordinance', 'charter', 'code', 'statute', 'regulation', 'footnote']:
                if dtype in text:
                    doc_type = dtype.title()
                    break
            break
    
    # Check for tables which might indicate reference material
    if not doc_type and soup.find('table'):
        table_headers = [th.get_text().strip().lower() for th in soup.find_all('th')]
        if table_headers:
            doc_type = 'Reference Table'
    
    # Check for footnotes
    if not doc_type and soup.find(class_='footnote-content'):
        doc_type = 'Footnote'
    
    # Default type if none detected
    if not doc_type:
        doc_type = 'Unknown'
    
    return doc_type

def analyze_sample_html_files(html_files, sample_size=20):
    """Analyze a sample of HTML files to identify structure patterns"""
    # Select a sample of HTML files
    sample_files = random.sample(html_files, min(sample_size, len(html_files)))
    
    schemas = defaultdict(list)
    doc_type_mapping = defaultdict(set)
    
    for file in sample_files:
        print(f"Analyzing {file}...")
        df = download_and_read_parquet(file)
        
        # Analyze each HTML content in the file
        for i, row in df.iterrows():
            if i >= 5:  # Limit to 5 rows per file for initial analysis
                break
                
            html_content = row['html']
            structure_info = extract_html_structure(html_content)
            
            # Try to identify document type
            doc_type = extract_document_type_from_html(html_content)
            
            # Map schema to document type
            doc_type_mapping[structure_info['signature_hash']].add(doc_type)
            
            # Store file and row info with the schema signature
            schemas[structure_info['signature_hash']].append({
                'file': file,
                'row_id': i,
                'cid': row['cid'],
                'doc_type': doc_type,
                'structure_info': structure_info
            })
    
    # Print summary of schemas found
    print(f"\nFound {len(schemas)} different HTML schema patterns")
    
    for i, (schema_hash, instances) in enumerate(sorted(schemas.items(), key=lambda x: len(x[1]), reverse=True)):
        print(f"\nSchema {i+1}: Hash {schema_hash} - Found in {len(instances)} instances")
        print(f"Document types: {', '.join(doc_type_mapping[schema_hash])}")
        print(f"Example file: {instances[0]['file']}, Row: {instances[0]['row_id']}")
        
        # Print common tag hierarchies for this schema
        common_patterns = instances[0]['structure_info']['hierarchy_patterns']
        if common_patterns:
            print("Common tag patterns:")
            for pattern, count in list(common_patterns.items())[:3]:
                print(f"  - {pattern}: {count} occurrences")
        
        # Print a sample of the actual HTML for the first instance
        if instances:
            file = instances[0]['file']
            row_id = instances[0]['row_id']
            df = download_and_read_parquet(file)
            html_sample = df.iloc[row_id]['html']
            
            # Print just the first 500 chars of HTML to see structure
            print(f"HTML Sample:\n{html_sample[:500]}...")
    
    return schemas, doc_type_mapping

def compare_citation_html_structure(html_files, citation_files, sample_size=10):
    """Compare citation and HTML data for the same files"""
    # Make sure we use matching pairs of files
    matching_pairs = []
    
    for html_file in html_files:
        base_name = html_file.replace('_html.parquet', '')
        citation_file = f"{base_name}_citation.parquet"
        
        if citation_file in citation_files:
            matching_pairs.append((html_file, citation_file))
    
    # Sample from the matching pairs
    if len(matching_pairs) > sample_size:
        sampled_pairs = random.sample(matching_pairs, sample_size)
    else:
        sampled_pairs = matching_pairs
    
    results = []
    
    for html_file, citation_file in sampled_pairs:
        print(f"Comparing {os.path.basename(html_file)} and {os.path.basename(citation_file)}...")
        
        html_df = download_and_read_parquet(html_file)
        citation_df = download_and_read_parquet(citation_file)
        
        # Check if they have the same number of rows
        html_rows = len(html_df)
        citation_rows = len(citation_df)
        
        # Check if cid values match between files
        html_cids = set(html_df['cid'].tolist())
        citation_cids = set(citation_df['cid'].tolist())
        
        matching_cids = html_cids & citation_cids
        
        results.append({
            'html_file': html_file,
            'citation_file': citation_file,
            'html_rows': html_rows,
            'citation_rows': citation_rows,
            'matching_cids': len(matching_cids),
            'html_only_cids': len(html_cids - citation_cids),
            'citation_only_cids': len(citation_cids - html_cids)
        })
    
    # Print summary
    print("\nHTML/Citation File Comparison:")
    for result in results:
        print(f"\n{os.path.basename(result['html_file'])} and {os.path.basename(result['citation_file'])}:")
        print(f"  HTML rows: {result['html_rows']}, Citation rows: {result['citation_rows']}")
        print(f"  Matching CIDs: {result['matching_cids']}")
        print(f"  HTML-only CIDs: {result['html_only_cids']}")
        print(f"  Citation-only CIDs: {result['citation_only_cids']}")
    
    return results

def main():
    print("Retrieving file list...")
    all_files = get_file_list()
    
    print(f"Total files: {len(all_files)}")
    
    html_files = get_html_files(all_files)
    citation_files = get_citation_files(all_files)
    metadata_files = get_metadata_files(all_files)
    
    print(f"HTML parquet files: {len(html_files)}")
    print(f"Citation parquet files: {len(citation_files)}")
    print(f"Metadata JSON files: {len(metadata_files)}")
    
    # Analyze sample HTML files to identify patterns
    print("\n=== HTML Schema Analysis ===")
    schemas, doc_type_mapping = analyze_sample_html_files(html_files, sample_size=15)
    
    # Analyze citation structure
    print("\n=== Citation Structure Analysis ===")
    if citation_files:
        sample_citation_file = random.choice(citation_files)
        print(f"Analyzing citation structure from {sample_citation_file}")
        citation_structure = analyze_citation_data(sample_citation_file)
        
        print("\nCitation Columns:")
        for col in citation_structure['columns']:
            presence = citation_structure['non_null_percentage'].get(col, 0)
            print(f"  - {col}: {presence:.1f}% non-null")
    
    # Compare HTML and citation files
    print("\n=== HTML/Citation Comparison ===")
    comparison_results = compare_citation_html_structure(html_files, citation_files, sample_size=5)
    
    # Output schema statistics to a file for further analysis
    schema_stats = {}
    for schema_hash, instances in schemas.items():
        schema_stats[schema_hash] = {
            'count': len(instances),
            'doc_types': list(doc_type_mapping[schema_hash]),
            'example_file': instances[0]['file'] if instances else None
        }
    
    with open('schema_stats.json', 'w') as f:
        json.dump(schema_stats, f, indent=2)
    
    print("\nSchema statistics saved to schema_stats.json")

if __name__ == "__main__":
    main() 