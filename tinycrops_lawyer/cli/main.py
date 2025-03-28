"""
Command-line interface for TinyCrops AI Lawyer system.
Provides various commands for processing and managing the American Law Dataset.
"""

import os
import sys
import argparse
import logging
import json
from datetime import datetime
import importlib.util

from tinycrops_lawyer.config import DEFAULT_MODEL
from tinycrops_lawyer.database import manager as db_manager
from tinycrops_lawyer.schema import manager as schema_manager
from tinycrops_lawyer.processing import workflow

# Initialize logger
logger = logging.getLogger(__name__)

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
    process_parser.add_argument('--model', default=DEFAULT_MODEL, help='LLM model to use')
    process_parser.add_argument('--reprocess-errors', action='store_true', help='Reprocess documents with errors')
    
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
    db_parser.add_argument('--import-metadata', action='store_true', help='Import documents from metadata')
    db_parser.add_argument('--state', help='Filter import by state code')
    
    # Report command
    report_parser = subparsers.add_parser('report', help='Generate reports')
    report_parser.add_argument('--progress', action='store_true', help='Generate progress report')
    
    # Initialize command (shortcut for fresh setup)
    init_parser = subparsers.add_parser('init', help='Initialize all components')
    
    return parser

def process_documents(args):
    """Process documents based on command line arguments"""
    logger.info(f"Starting document processing with model {args.model}")
    
    if args.reprocess_errors:
        # Reprocess documents with errors
        stats = workflow.reprocess_error_documents(
            limit=args.limit,
            model_name=args.model
        )
    else:
        # Process normal documents
        stats = workflow.process_batch(
            limit=args.limit,
            state_code=args.state,
            model_name=args.model
        )
    
    # Print results
    print("\nProcessing Results:")
    print(f"Processed: {stats['processed']} documents")
    print(f"Errors: {stats['errors']} documents")
    print(f"Success rate: {(stats['processed'] / (stats['processed'] + stats['errors']) * 100) if (stats['processed'] + stats['errors']) > 0 else 0:.2f}%")
    
    # Generate a report if possible
    try:
        visualize_progress()
        logger.info("Generated progress report")
    except Exception as e:
        logger.error(f"Failed to generate progress report: {e}")
    
    return stats['processed'] > 0

def manage_schemas(args):
    """Manage schemas based on command line arguments"""
    if args.analyze:
        logger.info(f"Analyzing document schemas (limit: {args.limit})")
        num_analyzed = workflow.analyze_schemas(limit=args.limit)
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
        for schema in stats['top_schemas'][:10]:  # Show top 10
            print(f"  {schema['short_id']}: {schema['document_count']} documents")
            print(f"    Types: {', '.join(schema['document_types'][:3])}")
    
    return True

def database_operations(args):
    """Perform database operations based on command line arguments"""
    if args.init:
        logger.info("Initializing database from scratch")
        success = workflow.initialize_database_from_scratch()
        if success:
            logger.info("Database initialized successfully")
        else:
            logger.error("Failed to initialize database")
    
    if args.import_metadata:
        logger.info(f"Importing documents from metadata (state: {args.state if args.state else 'all'})")
        stats = workflow.import_documents_from_metadata(state_code=args.state)
        print(f"\nImported {stats['imported']} documents out of {stats['total']} total")
        print(f"States: {stats['states']}")
        print(f"Places: {stats['places']}")
    
    if args.stats:
        logger.info("Generating database statistics")
        stats = db_manager.get_processing_statistics()
        
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
                  f"({doc_type['processed'] * 100 / doc_type['count']:.2f}% completed)")
        
        print("\nTop States:")
        for state in stats['state_stats'][:5]:  # Top 5 states
            print(f"  {state['state_name']} ({state['state_code']}): "
                  f"{state['processed_count']}/{state['document_count']} "
                  f"({state['percentage']:.2f}% completed)")
        
        print("\nRecent Processing Runs:")
        for run in stats['recent_runs'][:3]:  # Top 3 runs
            status_msg = f"Status: {run['status']}"
            if run['end_time']:
                duration = (datetime.fromisoformat(run['end_time']) - 
                           datetime.fromisoformat(run['start_time'])).total_seconds()
                status_msg += f", Duration: {duration:.2f}s"
            print(f"  Run {run['run_id']} ({run['start_time']}): {status_msg}")
            print(f"    Processed: {run['documents_processed']}, Errors: {run['errors_count']}, Model: {run['model_name']}")
    
    if args.backup:
        logger.info("Backing up database")
        backup_path = db_manager.backup_database()
        if backup_path:
            logger.info(f"Database backed up to {backup_path}")
        else:
            logger.error("Failed to back up database")
    
    if args.export:
        logger.info("Exporting database to CSV")
        exported_files = db_manager.export_to_csv()
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
            visualize_progress()
            logger.info("Generated progress report")
        except Exception as e:
            logger.error(f"Failed to generate progress report: {e}")
    
    return True

def initialize_all():
    """Initialize all components"""
    logger.info("Initializing all components")
    
    # Initialize database
    logger.info("1. Initializing database")
    if not db_manager.initialize_database():
        logger.error("Failed to initialize database")
        return False
    
    # Import documents from metadata
    logger.info("2. Importing documents from metadata")
    stats = workflow.import_documents_from_metadata()
    if stats["imported"] == 0:
        logger.error("Failed to import any documents")
        return False
    
    # Analyze schemas
    logger.info("3. Analyzing document schemas")
    num_analyzed = workflow.analyze_schemas(limit=100)
    logger.info(f"Analyzed {num_analyzed} documents")
    
    # Process initial batch
    logger.info("4. Processing initial batch of documents")
    process_stats = workflow.process_batch(limit=10)
    logger.info(f"Processed {process_stats['processed']} documents")
    
    # Generate reports
    logger.info("5. Generating reports")
    try:
        visualize_progress()
        logger.info("Generated progress report")
    except Exception as e:
        logger.error(f"Failed to generate progress report: {e}")
    
    logger.info("Initialization complete")
    return True

def visualize_progress():
    """Generate progress visualization"""
    try:
        # Try to import the visualize_progress module
        import importlib.util
        spec = importlib.util.find_spec('visualize_progress')
        if spec:
            import visualize_progress
            importlib.reload(visualize_progress)
            visualize_progress.main()
        else:
            logger.warning("visualize_progress module not found")
    except Exception as e:
        logger.error(f"Error generating progress visualization: {e}")

def main():
    """Main entry point for the CLI"""
    parser = setup_argparse()
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Execute the appropriate command
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