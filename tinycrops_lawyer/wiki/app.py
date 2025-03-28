"""
American Law Wiki Flask Application.
Serves the Wiki interface for the TinyCrops AI Lawyer system.
"""

import os
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from flask import Flask, render_template, request, redirect, url_for, abort, jsonify

from tinycrops_lawyer.config import OUTPUT_DIR
from tinycrops_lawyer.database import manager as db_manager

app = Flask(__name__, template_folder='../../templates')

@app.template_filter('urlencode')
def urlencode_filter(s):
    """Template filter to URL encode a string."""
    if isinstance(s, str):
        import urllib.parse
        return urllib.parse.quote_plus(s)
    return s

@app.template_filter('now')
def now_filter(format_string='Y'):
    """Template filter to get current date/time."""
    import datetime
    return datetime.datetime.now().strftime(format_string.replace('Y', '%Y'))

@app.template_filter('round')
def round_filter(value, precision=0):
    """Template filter to round numbers."""
    if value is None:
        return 0
    return round(value, precision)

@app.template_filter('int')
def int_filter(value):
    """Template filter to convert to integer."""
    if value is None:
        return 0
    return int(value)

@app.route('/wiki')
def home():
    """Home page for the American Law Wiki."""
    # Get database statistics
    stats = db_manager.get_processing_statistics()
    
    # Get latest processed documents
    session = db_manager.get_session()
    from tinycrops_lawyer.database.models import Document, State, Place
    from sqlalchemy import desc
    
    latest_docs_query = (
        session.query(
            Document,
            State.state_name,
            Place.place_name
        )
        .join(State, Document.state_code == State.state_code)
        .join(Place, Document.place_id == Place.place_id)
        .filter(Document.is_processed == True)
        .order_by(desc(Document.processed_date))
        .limit(5)
    )
    
    latest_documents = []
    for doc, state_name, place_name in latest_docs_query:
        latest_documents.append({
            'document_id': doc.document_id,
            'document_type': doc.document_type,
            'state_name': state_name,
            'place_name': place_name,
            'processed_date': doc.processed_date.strftime('%Y-%m-%d') if doc.processed_date else 'Unknown'
        })
    
    # Get document types for sidebar
    doc_types_query = stats['document_types'][:10]  # Top 10 document types
    
    session.close()
    
    return render_template(
        'wiki/home.html',
        stats=stats,
        latest_documents=latest_documents,
        document_types=doc_types_query
    )

@app.route('/wiki/states')
def states():
    """States list page."""
    states = db_manager.get_all_states()
    
    # Group states by region
    regions = {
        'Northeast': ['CT', 'ME', 'MA', 'NH', 'RI', 'VT', 'NJ', 'NY', 'PA'],
        'Midwest': ['IL', 'IN', 'MI', 'OH', 'WI', 'IA', 'KS', 'MN', 'MO', 'NE', 'ND', 'SD'],
        'South': ['DE', 'FL', 'GA', 'MD', 'NC', 'SC', 'VA', 'DC', 'WV', 'AL', 'KY', 'MS', 'TN', 'AR', 'LA', 'OK', 'TX'],
        'West': ['AZ', 'CO', 'ID', 'MT', 'NV', 'NM', 'UT', 'WY', 'AK', 'CA', 'HI', 'OR', 'WA']
    }
    
    states_by_region = {region: [] for region in regions}
    other_states = []
    
    for state in states:
        placed = False
        for region, state_codes in regions.items():
            if state['state_code'] in state_codes:
                states_by_region[region].append(state)
                placed = True
                break
        
        if not placed:
            other_states.append(state)
    
    # Sort states within each region
    for region in states_by_region:
        states_by_region[region].sort(key=lambda x: x['state_name'])
    
    if other_states:
        states_by_region['Other'] = sorted(other_states, key=lambda x: x['state_name'])
    
    return render_template(
        'wiki/states.html',
        states_by_region=states_by_region,
        total_states=len(states)
    )

@app.route('/wiki/state/<state_code>')
def state_detail(state_code):
    """State detail page."""
    state = db_manager.get_state(state_code)
    
    if not state:
        abort(404)
    
    # Get all places in this state
    session = db_manager.get_session()
    from tinycrops_lawyer.database.models import Place
    
    places_query = (
        session.query(Place)
        .filter(Place.state_code == state_code)
        .order_by(Place.place_name)
    )
    
    places = [
        {
            'place_id': place.place_id,
            'place_name': place.place_name,
            'place_type': place.place_type,
            'document_count': place.document_count,
            'processed_count': place.processed_count,
            'percentage': (place.processed_count / place.document_count * 100) if place.document_count > 0 else 0
        }
        for place in places_query
    ]
    
    # Get document types in this state
    from tinycrops_lawyer.database.models import Document
    from sqlalchemy import func
    
    doc_types_query = (
        session.query(
            Document.document_type,
            func.count(Document.id).label('count')
        )
        .filter(Document.state_code == state_code)
        .group_by(Document.document_type)
        .order_by(func.count(Document.id).desc())
    )
    
    document_types = [
        {
            'document_type': doc_type or 'Unknown',
            'count': count
        }
        for doc_type, count in doc_types_query
    ]
    
    session.close()
    
    return render_template(
        'wiki/state.html',
        state=state,
        places=places,
        document_types=document_types
    )

@app.route('/wiki/place/<place_id>')
def place_detail(place_id):
    """Place detail page."""
    session = db_manager.get_session()
    from tinycrops_lawyer.database.models import Place, State
    
    place_query = (
        session.query(Place, State.state_name)
        .join(State, Place.state_code == State.state_code)
        .filter(Place.place_id == place_id)
        .first()
    )
    
    if not place_query:
        session.close()
        abort(404)
    
    place, state_name = place_query
    
    # Get documents in this place
    from tinycrops_lawyer.database.models import Document
    from sqlalchemy import desc
    
    docs_query = (
        session.query(Document)
        .filter(Document.place_id == place_id)
        .filter(Document.is_processed == True)
        .order_by(desc(Document.processed_date))
        .limit(50)
    )
    
    documents = [
        {
            'document_id': doc.document_id,
            'document_type': doc.document_type or 'Unknown',
            'document_date': doc.document_date,
            'processed_date': doc.processed_date.strftime('%Y-%m-%d') if doc.processed_date else 'Unknown'
        }
        for doc in docs_query
    ]
    
    # Get document types count in this place
    from sqlalchemy import func
    
    doc_types_query = (
        session.query(
            Document.document_type,
            func.count(Document.id).label('count')
        )
        .filter(Document.place_id == place_id)
        .group_by(Document.document_type)
        .order_by(func.count(Document.id).desc())
    )
    
    document_types = [
        {
            'document_type': doc_type or 'Unknown',
            'count': count
        }
        for doc_type, count in doc_types_query
    ]
    
    place_info = {
        'place_id': place.place_id,
        'place_name': place.place_name,
        'place_type': place.place_type,
        'state_code': place.state_code,
        'state_name': state_name,
        'document_count': place.document_count,
        'processed_count': place.processed_count,
        'percentage': (place.processed_count / place.document_count * 100) if place.document_count > 0 else 0
    }
    
    session.close()
    
    return render_template(
        'wiki/place.html',
        place=place_info,
        documents=documents,
        document_types=document_types
    )

@app.route('/wiki/document_types')
def document_types():
    """Document types list page."""
    # Get document types and counts
    stats = db_manager.get_processing_statistics()
    document_types = stats['document_types']
    
    return render_template(
        'wiki/document_types.html',
        document_types=document_types
    )

@app.route('/wiki/document_types/<document_type>')
def document_type_detail(document_type):
    """Document type detail page."""
    session = db_manager.get_session()
    from tinycrops_lawyer.database.models import Document, State, Place
    from sqlalchemy import func, desc
    
    # Count documents with this type
    doc_count = (
        session.query(func.count(Document.id))
        .filter(Document.document_type == document_type)
        .scalar()
    )
    
    if doc_count == 0:
        session.close()
        abort(404)
    
    # Get example documents
    docs_query = (
        session.query(
            Document,
            State.state_name,
            Place.place_name
        )
        .join(State, Document.state_code == State.state_code)
        .join(Place, Document.place_id == Place.place_id)
        .filter(Document.document_type == document_type)
        .filter(Document.is_processed == True)
        .order_by(desc(Document.processed_date))
        .limit(20)
    )
    
    documents = [
        {
            'document_id': doc.document_id,
            'state_code': doc.state_code,
            'state_name': state_name,
            'place_name': place_name,
            'document_date': doc.document_date,
            'processed_date': doc.processed_date.strftime('%Y-%m-%d') if doc.processed_date else 'Unknown'
        }
        for doc, state_name, place_name in docs_query
    ]
    
    # Get states with this document type
    states_query = (
        session.query(
            State.state_code,
            State.state_name,
            func.count(Document.id).label('count')
        )
        .join(Document, Document.state_code == State.state_code)
        .filter(Document.document_type == document_type)
        .group_by(State.state_code, State.state_name)
        .order_by(func.count(Document.id).desc())
    )
    
    states = [
        {
            'state_code': state_code,
            'state_name': state_name,
            'count': count
        }
        for state_code, state_name, count in states_query
    ]
    
    processed_count = (
        session.query(func.count(Document.id))
        .filter(Document.document_type == document_type)
        .filter(Document.is_processed == True)
        .scalar()
    )
    
    session.close()
    
    return render_template(
        'wiki/document_type.html',
        document_type=document_type,
        total_count=doc_count,
        processed_count=processed_count,
        percentage=(processed_count / doc_count * 100) if doc_count > 0 else 0,
        documents=documents,
        states=states
    )

@app.route('/wiki/document/<document_id>')
def document_detail(document_id):
    """Document detail page."""
    session = db_manager.get_session()
    from tinycrops_lawyer.database.models import Document, State, Place
    
    doc_query = (
        session.query(
            Document,
            State.state_name,
            Place.place_name
        )
        .join(State, Document.state_code == State.state_code)
        .join(Place, Document.place_id == Place.place_id)
        .filter(Document.document_id == document_id)
        .first()
    )
    
    if not doc_query:
        session.close()
        abort(404)
    
    doc, state_name, place_name = doc_query
    
    if not doc.is_processed or not doc.processed_path:
        session.close()
        return render_template(
            'wiki/document_not_processed.html',
            document_id=document_id,
            document_type=doc.document_type,
            state_name=state_name,
            place_name=place_name
        )
    
    # Read the processed document
    try:
        with open(doc.processed_path, 'r', encoding='utf-8') as f:
            document_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        session.close()
        return render_template(
            'wiki/document_error.html',
            document_id=document_id,
            error="Could not read processed document file"
        )
    
    # Prepare document data
    document = {
        'document_id': doc.document_id,
        'document_type': doc.document_type or document_data.get('document_type', 'Unknown'),
        'jurisdiction': document_data.get('jurisdiction') or f"{place_name}, {state_name}",
        'state_code': doc.state_code,
        'state_name': state_name,
        'place_id': doc.place_id,
        'place_name': place_name,
        'date': doc.document_date or document_data.get('date'),
        'sections': document_data.get('sections', []),
        'processing_time': doc.processing_time,
        'processed_date': doc.processed_date.strftime('%Y-%m-%d %H:%M:%S') if doc.processed_date else 'Unknown'
    }
    
    session.close()
    
    return render_template(
        'wiki/document.html',
        document=document
    )

@app.route('/wiki/search')
def search():
    """Search documents."""
    query = request.args.get('q', '')
    
    if not query:
        return render_template(
            'wiki/search.html',
            query='',
            results=[],
            result_count=0
        )
    
    session = db_manager.get_session()
    from tinycrops_lawyer.database.models import Document, State, Place
    from sqlalchemy import or_, func
    
    # Simple search in document_type and document_id
    search_query = (
        session.query(
            Document,
            State.state_name,
            Place.place_name
        )
        .join(State, Document.state_code == State.state_code)
        .join(Place, Document.place_id == Place.place_id)
        .filter(
            or_(
                Document.document_type.ilike(f"%{query}%"),
                Document.document_id.ilike(f"%{query}%"),
                State.state_name.ilike(f"%{query}%"),
                Place.place_name.ilike(f"%{query}%")
            )
        )
        .filter(Document.is_processed == True)
        .limit(50)
    )
    
    results = [
        {
            'document_id': doc.document_id,
            'document_type': doc.document_type or 'Unknown',
            'state_code': doc.state_code,
            'state_name': state_name,
            'place_name': place_name,
            'processed_date': doc.processed_date.strftime('%Y-%m-%d') if doc.processed_date else 'Unknown'
        }
        for doc, state_name, place_name in search_query
    ]
    
    result_count = len(results)
    session.close()
    
    return render_template(
        'wiki/search.html',
        query=query,
        results=results,
        result_count=result_count
    )

@app.route('/wiki/latest')
def latest_documents():
    """Latest documents page."""
    session = db_manager.get_session()
    from tinycrops_lawyer.database.models import Document, State, Place
    from sqlalchemy import desc
    
    latest_query = (
        session.query(
            Document,
            State.state_name,
            Place.place_name
        )
        .join(State, Document.state_code == State.state_code)
        .join(Place, Document.place_id == Place.place_id)
        .filter(Document.is_processed == True)
        .order_by(desc(Document.processed_date))
        .limit(50)
    )
    
    documents = [
        {
            'document_id': doc.document_id,
            'document_type': doc.document_type or 'Unknown',
            'state_code': doc.state_code,
            'state_name': state_name,
            'place_name': place_name,
            'document_date': doc.document_date,
            'processed_date': doc.processed_date.strftime('%Y-%m-%d') if doc.processed_date else 'Unknown'
        }
        for doc, state_name, place_name in latest_query
    ]
    
    session.close()
    
    return render_template(
        'wiki/latest.html',
        documents=documents
    )

@app.route('/wiki/about')
def about():
    """About page."""
    # Get system info
    stats = db_manager.get_processing_statistics()
    
    return render_template(
        'wiki/about.html',
        stats=stats
    )

def main():
    """Run the Wiki Flask application."""
    app.run(host='0.0.0.0', port=5001, debug=True)

if __name__ == '__main__':
    main() 