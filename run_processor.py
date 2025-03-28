#!/usr/bin/env python
"""
Command-line utility for running the American Law Dataset processor
with various options and configurations.
"""

import os
import sys
import argparse
import logging
import json
from datetime import datetime
import importlib.util
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('processor.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('run_processor')

# Import our modules
try:
    import db_utils
    from location_processor import LocationProcessor
    from schema_manager import SchemaManager
    from schema_parser import SchemaParser
except ImportError as e:
    logger.error(f"Failed to import required modules: {e}")
    logger.error("Please ensure all required modules are installed.")
    sys.exit(1)

def setup_argparse():
    """Set up argument parser for command line options"""
    parser = argparse.ArgumentParser(
        description='Process American Law Dataset with various options',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Main command subparsers
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Process command
    process_parser = subparsers.add_parser('process', help='Process documents')
    process_parser.add_argument('--state', help='Process only documents from this state code')
    process_parser.add_argument('--limit', type=int, default=100, help='Max number of documents to process')
    process_parser.add_argument('--model', default='gemini-2.0-flash', help='LLM model to use')
    process_parser.add_argument('--batch-size', type=int, default=10, help='Batch size for processing')
    process_parser.add_argument('--only-new', action='store_true', help='Only process new documents')
    
    # Schema command
    schema_parser = subparsers.add_parser('schema', help='Manage document schemas')
    schema_parser.add_argument('--analyze', action='store_true', help='Analyze document schemas')
    schema_parser.add_argument('--limit', type=int, default=100, help='Max number of documents to analyze')
    schema_parser.add_argument('--export', action='store_true', help='Export schema samples')
    schema_parser.add_argument('--stats', action='store_true', help='Show schema statistics')
    
    # Database command
    db_parser = subparsers.add_parser('db', help='Database operations')
    db_parser.add_argument('--init', action='store_true', help='Initialize the database')
    db_parser.add_argument('--stats', action='store_true', help='Show database statistics')
    db_parser.add_argument('--backup', action='store_true', help='Backup the database')
    db_parser.add_argument('--export', action='store_true', help='Export database to CSV')
    
    # Report command
    report_parser = subparsers.add_parser('report', help='Generate reports')
    report_parser.add_argument('--progress', action='store_true', help='Generate progress report')
    
    # Initialize command
    init_parser = subparsers.add_parser('init', help='Initialize all components')
    
    return parser

def process_documents(args):
    """Process documents based on command line arguments"""
    logger.info(f"Starting document processing with model {args.model}")
    
    # Initialize processor
    processor = LocationProcessor(model_name=args.model, batch_size=args.batch_size)
    
    # Initialize database if needed
    db_utils.initialize_database()
    
    # Start a processing run
    run_id = db_utils.start_processing_run(
        model_name=args.model,
        notes=f"State: {args.state if args.state else 'all'}, Limit: {args.limit}"
    )
    
    start_time = time.time()
    
    try:
        # Process documents
        if args.state:
            logger.info(f"Processing documents for state {args.state}")
            processed = processor.process_documents_by_jurisdiction(
                state_code=args.state,
                limit=args.limit,
                only_new=args.only_new
            )
        else:
            logger.info("Processing documents for all states")
            processed = processor.process_documents_by_jurisdiction(
                limit=args.limit,
                only_new=args.only_new
            )
        
        # End the processing run
        processing_time = time.time() - start_time
        db_utils.end_processing_run(
            run_id=run_id,
            status="completed",
            documents_processed=processed['processed'],
            errors_count=processed['errors']
        )
        
        logger.info(f"Document processing completed in {processing_time:.2f} seconds")
        logger.info(f"Processed {processed['processed']} documents with {processed['errors']} errors")
        
        # Generate a report
        try:
            import visualize_progress
            importlib.reload(visualize_progress)
            visualize_progress.main()
            logger.info("Generated progress report")
        except Exception as e:
            logger.error(f"Failed to generate progress report: {e}")
        
    except Exception as e:
        logger.error(f"Error during document processing: {e}")
        
        # End the processing run with error status
        db_utils.end_processing_run(
            run_id=run_id,
            status="error",
            documents_processed=0,
            errors_count=1
        )
        
        return False
    
    return True

def manage_schemas(args):
    """Manage schemas based on command line arguments"""
    schema_manager = SchemaManager()
    
    if args.analyze:
        logger.info(f"Analyzing document schemas (limit: {args.limit})")
        num_analyzed = schema_manager.batch_analyze_documents(limit=args.limit)
        logger.info(f"Analyzed {num_analyzed} documents")
    
    if args.export:
        logger.info("Exporting schema samples")
        samples_dir = schema_manager.export_schema_samples()
        logger.info(f"Exported schema samples to {samples_dir}")
    
    if args.stats:
        logger.info("Generating schema statistics")
        stats = schema_manager.get_schema_statistics()
        print("\nSchema Statistics:")
        print(f"Total schemas: {stats['total_schemas']}")
        print(f"Total documents analyzed: {stats['total_documents_analyzed']}")
        print("\nDocument Types:")
        for doc_type, count in stats['document_types'].items():
            print(f"  {doc_type}: {count}")
        
        print("\nTop Schemas:")
        for schema in stats['top_schemas']:
            print(f"  {schema['hash'][:8]}: {schema['document_count']} documents, types: {schema['document_types']}")
    
    return True

def database_operations(args):
    """Perform database operations based on command line arguments"""
    if args.init:
        logger.info("Initializing database")
        success = db_utils.initialize_database()
        if success:
            logger.info("Database initialized successfully")
        else:
            logger.error("Failed to initialize database")
    
    if args.stats:
        logger.info("Generating database statistics")
        stats = db_utils.get_processing_statistics()
        
        print("\nDatabase Statistics:")
        print(f"Total documents: {stats['total_documents']}")
        print(f"Processed documents: {stats['processed_documents']}")
        print(f"Completion: {stats['overall_percentage']:.2f}%")
        print(f"States: {stats['states_count']}")
        print(f"Places: {stats['places_count']}")
        
        print("\nDocument Types:")
        for doc_type in stats['document_types'][:5]:  # Top 5 document types
            print(f"  {doc_type['document_type'] or 'unknown'}: "
                  f"{doc_type['processed']}/{doc_type['count']} "
                  f"({doc_type['processed'] * 100 / doc_type['count']:.2f}%)")
        
        print("\nTop States:")
        for state in stats['state_stats'][:5]:  # Top 5 states
            print(f"  {state['state_name']} ({state['state_code']}): "
                  f"{state['processed_count']}/{state['document_count']} "
                  f"({state['percentage']:.2f}%)")
        
        print("\nRecent Processing Runs:")
        for run in stats['recent_runs'][:3]:  # Top 3 runs
            status_msg = f"Status: {run['status']}"
            if run['end_time']:
                status_msg += f", Duration: {(datetime.fromisoformat(run['end_time']) - datetime.fromisoformat(run['start_time'])).total_seconds():.2f}s"
            print(f"  Run {run['run_id']} ({run['start_time']}): {status_msg}")
            print(f"    Processed: {run['documents_processed']}, Errors: {run['errors_count']}, Model: {run['model_name']}")
    
    if args.backup:
        logger.info("Backing up database")
        backup_path = db_utils.backup_database()
        if backup_path:
            logger.info(f"Database backed up to {backup_path}")
        else:
            logger.error("Failed to back up database")
    
    if args.export:
        logger.info("Exporting database to CSV")
        exported_files = db_utils.export_to_csv()
        if exported_files:
            logger.info(f"Database tables exported to:")
            for table, path in exported_files.items():
                logger.info(f"  {table}: {path}")
        else:
            logger.error("Failed to export database")
    
    return True

def generate_reports(args):
    """Generate reports based on command line arguments"""
    if args.progress:
        logger.info("Generating progress report")
        try:
            import visualize_progress
            importlib.reload(visualize_progress)
            visualize_progress.main()
            logger.info("Generated progress report")
        except Exception as e:
            logger.error(f"Failed to generate progress report: {e}")
            return False
    
    return True

def initialize_all():
    """Initialize all components"""
    logger.info("Initializing all components")
    
    # Initialize database
    logger.info("Initializing database")
    db_success = db_utils.initialize_database()
    if db_success:
        logger.info("Database initialized successfully")
    else:
        logger.error("Failed to initialize database")
        return False
    
    # Set up directory structure
    directories = [
        "data_cache",
        "schema_cache",
        "processed_documents",
        "reports",
        "db_backups"
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        logger.info(f"Created directory: {directory}")
    
    # Create initial backup
    backup_path = db_utils.backup_database()
    if backup_path:
        logger.info(f"Created initial database backup: {backup_path}")
    
    # Create version info file
    version_info = {
        "version": "1.0.0",
        "initialized_date": datetime.now().isoformat(),
        "python_version": sys.version,
        "components": [
            "location_processor.py",
            "schema_manager.py",
            "schema_parser.py", 
            "db_utils.py",
            "visualize_progress.py"
        ]
    }
    
    with open("version_info.json", "w") as f:
        json.dump(version_info, f, indent=2)
    
    logger.info("Created version info file")
    
    logger.info("Initialization complete")
    return True

def main():
    """Main function to handle command line arguments and run the processor"""
    parser = setup_argparse()
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Handle commands
    if args.command == 'process':
        process_documents(args)
    elif args.command == 'schema':
        manage_schemas(args)
    elif args.command == 'db':
        database_operations(args)
    elif args.command == 'report':
        generate_reports(args)
    elif args.command == 'init':
        initialize_all()
    else:
        parser.print_help()

if __name__ == "__main__":
    main() 