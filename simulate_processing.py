#!/usr/bin/env python
"""
Simulate document processing activity for the American Law Dataset.
This will randomly select unprocessed documents and mark them as processed,
updating the database to show progress over time.
"""

import os
import db_utils
import json
import random
import time
import argparse
import datetime
from datetime import timedelta
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='simulate_processing.log'
)
logger = logging.getLogger('simulate_processing')

def simulate_processing_batch(batch_size=10, delay=5, error_rate=0.1):
    """
    Simulate processing a batch of documents
    
    Args:
        batch_size: Number of documents to process in this batch
        delay: Delay between document processing (seconds)
        error_rate: Probability of a processing error (0-1)
        
    Returns:
        Tuple of (processed_count, error_count)
    """
    # Start a processing run
    run_id = db_utils.start_processing_run('gemini-2.0-flash', 'Simulated processing run')
    
    if not run_id:
        logger.error("Failed to start processing run")
        return 0, 0
    
    # Get unprocessed documents
    documents = db_utils.get_unprocessed_documents(limit=batch_size)
    
    if not documents:
        logger.info("No unprocessed documents found")
        db_utils.end_processing_run(run_id, 'completed', 0, 0)
        return 0, 0
    
    processed_count = 0
    error_count = 0
    
    # Process documents
    for doc in documents:
        # Simulate processing time
        processing_time = random.uniform(0.5, 3.0)
        time.sleep(delay)  # Add delay between documents
        
        document_id = doc['document_id']
        processed_path = f"processed_documents/{document_id}.json"
        
        # Create processed file directory if it doesn't exist
        os.makedirs(os.path.dirname(processed_path), exist_ok=True)
        
        # Determine if there's an error
        error = None
        if random.random() < error_rate:
            error_count += 1
            error = f"Simulated processing error for document {document_id}"
            logger.warning(error)
        else:
            processed_count += 1
            # Create a sample processed file
            processed_data = {
                'document_id': document_id,
                'processed_date': datetime.datetime.now().isoformat(),
                'processing_time': processing_time,
                'document_type': doc['document_type'],
                'jurisdiction': {
                    'state_code': doc['state_code'],
                    'place_id': doc['place_id']
                },
                'content': {
                    'title': f"Simulated processed document {document_id}",
                    'sections': [
                        {'id': '1', 'title': 'Section 1', 'content': 'This is section 1 content.'},
                        {'id': '2', 'title': 'Section 2', 'content': 'This is section 2 content.'},
                        {'id': '3', 'title': 'Section 3', 'content': 'This is section 3 content.'}
                    ]
                }
            }
            
            with open(processed_path, 'w') as f:
                json.dump(processed_data, f, indent=2)
            
            logger.info(f"Processed document {document_id}")
        
        # Mark document as processed
        db_utils.mark_document_processed(document_id, processed_path, processing_time, error)
    
    # End the processing run
    db_utils.end_processing_run(run_id, 'completed', processed_count, error_count)
    
    logger.info(f"Completed batch: {processed_count} processed, {error_count} errors")
    return processed_count, error_count

def main():
    """Main function to simulate document processing"""
    parser = argparse.ArgumentParser(description='Simulate document processing')
    parser.add_argument('--batches', type=int, default=5, help='Number of batches to process')
    parser.add_argument('--batch-size', type=int, default=10, help='Number of documents per batch')
    parser.add_argument('--batch-delay', type=int, default=5, help='Delay between batches in seconds')
    parser.add_argument('--doc-delay', type=float, default=1.0, help='Delay between documents in seconds')
    parser.add_argument('--error-rate', type=float, default=0.1, help='Probability of processing error (0-1)')
    parser.add_argument('--continuous', action='store_true', help='Run continuously until stopped')
    
    args = parser.parse_args()
    
    total_processed = 0
    total_errors = 0
    batch_count = 0
    
    try:
        if args.continuous:
            logger.info("Starting continuous processing simulation")
            print("Starting continuous processing simulation. Press Ctrl+C to stop.")
            
            while True:
                batch_count += 1
                print(f"Processing batch {batch_count}...")
                processed, errors = simulate_processing_batch(args.batch_size, args.doc_delay, args.error_rate)
                total_processed += processed
                total_errors += errors
                
                if processed == 0:
                    print("All documents processed. Exiting.")
                    break
                
                print(f"Batch {batch_count} complete: {processed} processed, {errors} errors")
                print(f"Waiting {args.batch_delay} seconds until next batch...")
                time.sleep(args.batch_delay)
        else:
            logger.info(f"Starting processing simulation for {args.batches} batches")
            print(f"Starting processing simulation for {args.batches} batches")
            
            for i in range(args.batches):
                batch_count += 1
                print(f"Processing batch {batch_count} of {args.batches}...")
                processed, errors = simulate_processing_batch(args.batch_size, args.doc_delay, args.error_rate)
                total_processed += processed
                total_errors += errors
                
                if processed == 0:
                    print("All documents processed. Exiting.")
                    break
                
                if i < args.batches - 1:
                    print(f"Waiting {args.batch_delay} seconds until next batch...")
                    time.sleep(args.batch_delay)
    
    except KeyboardInterrupt:
        print("\nProcessing simulation stopped by user")
    
    logger.info(f"Processing simulation complete: {total_processed} documents processed, {total_errors} errors in {batch_count} batches")
    print(f"\nProcessing simulation complete: {total_processed} documents processed, {total_errors} errors in {batch_count} batches")

if __name__ == '__main__':
    main() 