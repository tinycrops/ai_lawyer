#!/usr/bin/env python
"""
Script to explore the 'the-ride-never-ends/american_law' dataset on HuggingFace
and extract sample rows for understanding its structure.
"""
import os
import sys
import json
import random
from pathlib import Path
from typing import Dict, List, Any, Optional
from collections import Counter

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from datasets import load_dataset
from tqdm import tqdm
from pprint import pprint


def explore_hf_dataset():
    """
    Explore the dataset directly using the datasets library.
    """
    print("Loading the HuggingFace dataset 'the-ride-never-ends/american_law'...")
    try:
        dataset = load_dataset("the-ride-never-ends/american_law")
        print(f"\nDataset structure:")
        print(f"- Splits: {list(dataset.keys())}")
        
        # Get info about each split
        for split_name, split_data in dataset.items():
            print(f"\n- Split '{split_name}':")
            print(f"  - Number of rows: {len(split_data)}")
            print(f"  - Features: {split_data.features}")
            print(f"  - Column names: {split_data.column_names}")
            
        # Return the dataset for further exploration
        return dataset
        
    except Exception as e:
        print(f"Error loading dataset: {e}")
        print("Attempting to load with streaming=True...")
        try:
            dataset = load_dataset("the-ride-never-ends/american_law", streaming=True)
            print(f"\nSuccessfully loaded dataset in streaming mode")
            print(f"- Splits: {list(dataset.keys())}")
            return dataset
        except Exception as e2:
            print(f"Error loading dataset in streaming mode: {e2}")
            return None


def sample_random_rows(dataset, num_samples=30):
    """
    Sample random rows from the dataset for analysis.
    """
    if not dataset:
        print("No dataset available for sampling.")
        return []
    
    samples = []
    
    # Get the main split (usually 'train')
    main_split = next(iter(dataset.keys()))
    split_data = dataset[main_split]
    
    # Check if we're in streaming mode
    is_streaming = hasattr(split_data, 'take')
    
    try:
        if is_streaming:
            # For streaming dataset
            # Take first 1000 rows to sample from
            buffer = list(split_data.take(1000))
            
            # Sample from buffer
            if buffer:
                indices = random.sample(range(len(buffer)), min(num_samples, len(buffer)))
                samples = [buffer[i] for i in indices]
        else:
            # For regular dataset
            # Get random indices
            indices = random.sample(range(len(split_data)), min(num_samples, len(split_data)))
            
            # Get samples
            for idx in indices:
                samples.append(split_data[idx])
    
    except Exception as e:
        print(f"Error sampling data: {e}")
        # Try another approach for streaming datasets
        try:
            if is_streaming:
                samples = list(split_data.take(num_samples))
        except Exception as e2:
            print(f"Error with alternative sampling approach: {e2}")
    
    return samples


def save_samples(samples, output_file="dataset_samples.json"):
    """
    Save sample data to a JSON file.
    """
    # Convert samples to serializable format
    serializable_samples = []
    for sample in samples:
        # Convert each sample to dict and handle non-serializable types
        sample_dict = {}
        for key, value in sample.items():
            # Handle bytes and other non-serializable types
            if isinstance(value, bytes):
                sample_dict[key] = "<binary data>"
            else:
                try:
                    # Test if the value is JSON serializable
                    json.dumps(value)
                    sample_dict[key] = value
                except (TypeError, OverflowError):
                    sample_dict[key] = str(value)
        
        serializable_samples.append(sample_dict)
    
    with open(output_file, 'w') as f:
        json.dump(serializable_samples, f, indent=2)
    print(f"Saved {len(serializable_samples)} samples to {output_file}")
    return serializable_samples


def analyze_samples(samples):
    """
    Analyze the samples to understand the dataset structure.
    """
    if not samples:
        print("No samples to analyze.")
        return
    
    print("\nSample Analysis:")
    
    # Get all keys across all samples
    all_keys = set()
    for sample in samples:
        all_keys.update(sample.keys())
    
    print(f"- Columns found: {', '.join(sorted(all_keys))}")
    
    # Analyze key data types and value distributions
    key_stats = {}
    for key in all_keys:
        values = [sample.get(key) for sample in samples if key in sample]
        types = [type(val).__name__ for val in values if val is not None]
        
        key_stats[key] = {
            "count": len(values),
            "types": list(set(types)),
            "null_count": sum(1 for val in values if val is None),
        }
        
        # For string values, add some stats
        str_values = [val for val in values if isinstance(val, str)]
        if str_values:
            key_stats[key]["avg_length"] = sum(len(val) for val in str_values) / len(str_values)
        
    print("\nColumn Statistics:")
    for key, stats in key_stats.items():
        print(f"- {key}:")
        print(f"  - Present in {stats['count']}/{len(samples)} samples")
        print(f"  - Types: {', '.join(stats['types'])}")
        print(f"  - Null count: {stats['null_count']}")
        if "avg_length" in stats:
            print(f"  - Average string length: {stats['avg_length']:.1f}")
    
    return key_stats


def analyze_location_data(samples):
    """
    Analyze the location-specific data in the samples.
    """
    if not samples:
        print("No samples to analyze.")
        return
    
    print("\nLocation Analysis:")
    
    # Extract location information
    locations = {}
    states = {}
    
    for sample in samples:
        # Extract location data
        place_name = sample.get('place_name', '')
        state_code = sample.get('state_code', '')
        state_name = sample.get('state_name', '')
        
        # Skip if no place name
        if not place_name:
            continue
        
        # Add to locations
        if place_name not in locations:
            locations[place_name] = {
                "state_code": state_code,
                "state_name": state_name,
                "count": 0,
                "documents": []
            }
        
        locations[place_name]["count"] += 1
        
        cid = sample.get('cid', '')
        if cid and cid not in locations[place_name]["documents"]:
            locations[place_name]["documents"].append(cid)
        
        # Add to states
        if state_code:
            if state_code not in states:
                states[state_code] = {
                    "state_name": state_name,
                    "locations": set(),
                    "count": 0
                }
            
            states[state_code]["count"] += 1
            states[state_code]["locations"].add(place_name)
    
    # Print location stats
    print(f"- Found {len(locations)} distinct locations")
    print(f"- Found {len(states)} distinct states")
    
    if locations:
        print("\nTop 5 locations by document count:")
        top_locations = sorted(locations.items(), key=lambda x: x[1]["count"], reverse=True)[:5]
        for place_name, data in top_locations:
            print(f"  - {place_name} ({data['state_code']}): {data['count']} documents")
    
    if states:
        print("\nStates by location count:")
        for state_code, data in sorted(states.items(), key=lambda x: len(x[1]["locations"]), reverse=True):
            print(f"  - {state_code} ({data['state_name']}): {len(data['locations'])} locations, {data['count']} documents")
    
    return {"locations": locations, "states": states}


def analyze_document_structure(samples):
    """
    Analyze the document structure to understand chapter and title organization.
    """
    if not samples:
        print("No samples to analyze.")
        return
    
    print("\nDocument Structure Analysis:")
    
    # Extract document structure information
    chapters = {}
    titles = {}
    
    for sample in samples:
        # Extract document data
        chapter = sample.get('chapter', '')
        chapter_num = sample.get('chapter_num', '')
        title = sample.get('title', '')
        title_num = sample.get('title_num', '')
        
        # Process chapter information
        if chapter:
            if chapter not in chapters:
                chapters[chapter] = {
                    "chapter_num": chapter_num,
                    "count": 0,
                    "documents": []
                }
            
            chapters[chapter]["count"] += 1
            
            cid = sample.get('cid', '')
            if cid and cid not in chapters[chapter]["documents"]:
                chapters[chapter]["documents"].append(cid)
        
        # Process title information
        if title:
            if title not in titles:
                titles[title] = {
                    "title_num": title_num,
                    "count": 0,
                    "documents": []
                }
            
            titles[title]["count"] += 1
            
            cid = sample.get('cid', '')
            if cid and cid not in titles[title]["documents"]:
                titles[title]["documents"].append(cid)
    
    # Print document structure stats
    print(f"- Found {len(chapters)} distinct chapters")
    print(f"- Found {len(titles)} distinct titles")
    
    if chapters:
        print("\nTop 5 chapters by document count:")
        top_chapters = sorted(chapters.items(), key=lambda x: x[1]["count"], reverse=True)[:5]
        for chapter_name, data in top_chapters:
            print(f"  - {chapter_name}: {data['count']} documents")
    
    if titles:
        print("\nTop 5 titles by document count:")
        top_titles = sorted(titles.items(), key=lambda x: x[1]["count"], reverse=True)[:5]
        for title_name, data in top_titles:
            print(f"  - {title_name}: {data['count']} documents")
    
    return {"chapters": chapters, "titles": titles}


def analyze_temporal_data(samples):
    """
    Analyze the temporal aspects of the data.
    """
    if not samples:
        print("No samples to analyze.")
        return
    
    print("\nTemporal Data Analysis:")
    
    # Extract date and year information
    dates = {}
    years = {}
    
    for sample in samples:
        # Extract date data
        date = sample.get('date', '')
        year = sample.get('year')
        enacted = sample.get('enacted')
        
        # Process date information
        if date:
            if date not in dates:
                dates[date] = {
                    "count": 0,
                    "documents": []
                }
            
            dates[date]["count"] += 1
            
            cid = sample.get('cid', '')
            if cid and cid not in dates[date]["documents"]:
                dates[date]["documents"].append(cid)
        
        # Process year information
        # Try to extract year from date if year is not available
        if not year and date:
            import re
            year_match = re.search(r'(\d{4})', str(date))
            if year_match:
                year = year_match.group(1)
        
        if year:
            if year not in years:
                years[year] = {
                    "count": 0,
                    "documents": []
                }
            
            years[year]["count"] += 1
            
            cid = sample.get('cid', '')
            if cid and cid not in years[year]["documents"]:
                years[year]["documents"].append(cid)
    
    # Print temporal stats
    print(f"- Found {len(dates)} distinct dates")
    print(f"- Found {len(years)} distinct years")
    
    if years:
        print("\nTop 5 years by document count:")
        top_years = sorted(years.items(), key=lambda x: x[1]["count"], reverse=True)[:5]
        for year, data in top_years:
            print(f"  - {year}: {data['count']} documents")
    
    # Try to determine year range
    if years:
        year_values = [int(year) for year in years.keys() if year and year.isdigit()]
        if year_values:
            print(f"\nYear range: {min(year_values)} - {max(year_values)}")
    
    return {"dates": dates, "years": years}


def main():
    # Explore dataset structure
    dataset = explore_hf_dataset()
    
    # Sample random rows
    print("\nSampling random rows from the dataset...")
    samples = sample_random_rows(dataset, num_samples=100)
    
    # Save samples
    serialized_samples = save_samples(samples, output_file=os.path.join(Path(__file__).parent, "dataset_samples.json"))
    
    # Analyze samples
    key_stats = analyze_samples(serialized_samples)
    
    # Location-based analysis
    location_stats = analyze_location_data(serialized_samples)
    
    # Document structure analysis
    doc_structure = analyze_document_structure(serialized_samples)
    
    # Temporal analysis
    temporal_stats = analyze_temporal_data(serialized_samples)
    
    # Save combined analysis results
    analysis_results = {
        "key_stats": key_stats,
        "location_stats": location_stats,
        "document_structure": doc_structure,
        "temporal_stats": temporal_stats
    }
    
    analysis_file = os.path.join(Path(__file__).parent, "dataset_analysis.json")
    with open(analysis_file, 'w') as f:
        # Convert sets to lists for JSON serialization
        analysis_json = json.dumps(analysis_results, default=lambda x: list(x) if isinstance(x, set) else x, indent=2)
        f.write(analysis_json)
    
    print(f"\nSaved detailed analysis to {analysis_file}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc() 