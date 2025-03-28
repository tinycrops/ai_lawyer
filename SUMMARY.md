# American Law Dataset Parser - Processing Plan

## Dataset Overview

The "the-ride-never-ends/american_law" dataset from Hugging Face contains a large collection of American legal documents from various municipalities, counties, and jurisdictions:

- **Size**: Over 2,700 files with more than 1 million document segments
- **Structure**:
  - HTML files (*.html.parquet): ~935 files containing raw document content
  - Citation files (*.citation.parquet): ~847 files with metadata and references
  - Metadata files (*.json): ~935 files with additional document information
  
- **Document Types**: 
  - Ordinances - Local laws and regulations
  - Codes - Codified municipal and county laws
  - Regulations - Administrative rules
  - Charters - Founding documents for municipalities
  - Footnotes - Explanatory text

## Processing Roadmap

### 1. Location-Based Processing

Since legal documents are organized by jurisdiction (place_name), we will structure our processing pipeline around geographic entities:

1. **Group by State**:
   - Create state-level aggregation points
   - Organize documents by state_code (e.g., NC, CA)

2. **Subdivide by Municipality/County**:
   - Further organize by place_name within each state
   - Create jurisdiction-specific document collections

3. **Temporal Organization**:
   - Within each jurisdiction, organize by enactment date/year
   - Create historical timeline of legal developments

### 2. Document Processing Pipeline

For each jurisdiction, we will process documents through the following steps:

#### A. Schema Detection & HTML Parsing

1. **Schema Identification**:
   - Use the existing schema detection mechanism
   - Group documents by similar HTML structures
   - Create schema-specific parsers

2. **Content Extraction**:
   - Extract text content from HTML
   - Preserve document structure and hierarchies
   - Handle tables, lists, and special formatting

3. **Document Type Classification**:
   - Classify documents by type (ordinance, code, etc.)
   - Apply type-specific processing rules

#### B. LLM-Powered Normalization

1. **Schema-to-JSON Translation**:
   - Use Gemini 2.0 Flash to interpret each document structure
   - Create prompts with schema context and sample parsing patterns
   - Generate structured JSON output

2. **Prompt Engineering**:
   ```
   PROMPT TEMPLATE:
   You are a legal document parser specializing in American municipal law.
   
   DOCUMENT SCHEMA:
   {schema_signature}
   
   DOCUMENT TYPE:
   {document_type}
   
   SOURCE JURISDICTION:
   {place_name}, {state_name}
   
   HTML CONTENT:
   {html_content}
   
   PARSING INSTRUCTIONS:
   1. Extract the main document content, preserving section structure
   2. Identify section headings, numbers, and content
   3. Format date references consistently as YYYY-MM-DD
   4. Extract any legislative history or citation information
   5. Preserve footnotes and references
   
   OUTPUT FORMAT:
   Return a JSON object with the following fields:
   - document_id: Unique identifier
   - jurisdiction: Place and state information
   - document_type: The type of document
   - sections: Array of section objects containing:
     - section_id: Unique section identifier
     - section_num: The section number
     - section_title: The section heading
     - section_text: The main text content
     - section_refs: Any references to other sections or documents
   ```

3. **Quality Control**:
   - Validate JSON output
   - Check for missing sections or content
   - Ensure proper handling of complex formatting

#### C. Document Integration

1. **Section Linking**:
   - Establish connections between related sections
   - Link references across documents
   - Create a navigable document graph

2. **Citation Processing**:
   - Connect citation metadata with document content
   - Standardize citation formats
   - Enable citation-based searches

3. **Metadata Enhancement**:
   - Enrich documents with additional metadata
   - Standardize dates, jurisdiction names, and document types

### 3. Storage & Access Layer

1. **Document Database**:
   - Organize processed documents in SQLite
   - Use document_id and section_id for fast lookups
   - Store full-text for search capabilities

2. **Geographic Indexing**:
   - Create state and locality-based indexes
   - Enable jurisdiction filtering
   - Support cross-jurisdiction searches

3. **Document Type Collections**:
   - Group documents by type
   - Enable filtering by ordinance, code, regulation, etc.
   - Provide specialized views for different document types

4. **JSON Export Formats**:
   - Single document export
   - Jurisdiction collection export
   - Complete corpus export

## Implementation Plan

### Phase 1: Infrastructure (Week 1)

- Set up processing database
- Implement document tracking
- Create schema detection pipeline
- Build cache and storage mechanisms

### Phase 2: Parsing Engine (Weeks 2-3)

- Develop LLM prompt templates
- Create schema-specific parsers
- Implement quality control checks
- Build document section processor

### Phase 3: Document Organization (Weeks 4-5)

- Implement location-based organization
- Create document type collections
- Build citation network
- Develop document linking

### Phase 4: Access & Visualization (Weeks 6-7)

- Create jurisdiction-based browsers
- Implement search functionality
- Build document comparison tools
- Develop temporal visualization

## Monitoring & Evaluation

- Track processing success rates by document type
- Monitor schema coverage and parser effectiveness
- Evaluate LLM performance across different document schemas
- Measure completeness of document collections by jurisdiction

## Reporting

- Generate processing statistics by jurisdiction
- Create document coverage maps
- Report on document type distributions
- Track historical coverage timeline 