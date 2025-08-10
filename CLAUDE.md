# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

NumHarvest is a Python-based web scraping framework for extracting phone numbers and area codes from excellentnumbers.com. The project uses a modular architecture with abstract base classes and concrete implementations for different scraping phases.

## Common Commands

### Environment Setup
```bash
# Setup virtual environment
python -m venv venv
source venv/bin/activate  # macOS/Linux
# or
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers (required for web scraping)
playwright install
```

### Running Scrapers
```bash
# Phase 1: Collect area codes and regions from the website
python excellentnumbers_scraper.py

# Phase 2: Extract phone numbers using collected data
python numbers_extractor.py

# Alternative execution method
python -c "import asyncio; from numbers_extractor import main; asyncio.run(main())"
```

## Architecture Overview

### Core Framework (Template Method Pattern)
- **BaseScraper** (`base_scraper.py`): Abstract base class defining the scraping workflow
  - `fetch_data()`: Web data collection
  - `parse_data()`: Data processing and structuring  
  - `store_data()`: Data persistence
  - `execute()`: Orchestrates complete workflow

### Two-Phase Scraping Architecture

#### Phase 1: Region Discovery
- **ExcellentnumbersScraper** (`excellentnumbers_scraper.py`)
- Uses Playwright for web automation
- Extracts area codes and regional categories from excellentnumbers.com
- Outputs structured data to `excellentnumbers_data.json`
- Removes previous US-only limitations to support global regions

#### Phase 2: Phone Number Extraction  
- **NumbersExtractor** (`numbers_extractor.py`)
- Reads cached region data from Phase 1
- Performs multi-page scraping with automatic pagination
- Real-time MongoDB storage (one document per phone number)
- Handles duplicate detection and data deduplication

### Data Flow
```
Web Scraping → JSON Cache → URL Generation → Phone Extraction → MongoDB Storage
```

## MongoDB Integration

### Configuration
- Host: `43.159.58.235:27017`
- Database: `extra_numbers` 
- Collection: `extra_numbers`
- Authentication: Uses `extra_numbers` user with provided credentials

### Document Schema
```json
{
  "number": "(xxx) xxx-xxxx",
  "price": "$xxx.xx", 
  "region": "State/Province",
  "area_code": "xxx",
  "page": 1,
  "source_url": "https://...",
  "created_at": "ISO_DATE",
  "updated_at": "ISO_DATE"
}
```

### Indexing Strategy
- Unique index on `number` field (prevents duplicates)
- Compound index on `(region, area_code)` (query optimization)

## Key Implementation Details

### Web Scraping Patterns
- All URLs use `?sort=newest&sortcode=` parameter for consistent sorting
- Maximum 50 pages per area code (prevents infinite loops)
- Built-in rate limiting with 1-3 second delays
- Robust error handling for network issues and page structure changes

### Phone Number Extraction
- Uses regex pattern matching for `(xxx) xxx-xxxx` format
- Extracts pricing information alongside phone numbers
- Handles multiple page layouts and element structures
- Implements fallback extraction methods

### Data Storage Strategy  
- **Immediate persistence**: Each page's data saved to MongoDB immediately
- **Batch operations**: Uses `insert_many()` for efficiency
- **Duplicate handling**: Graceful handling of duplicate key errors
- **Connection management**: Proper MongoDB connection lifecycle

## Dependencies

### Core Requirements
- `playwright>=1.40.0`: Web automation framework
- `pymongo`: MongoDB integration (imported in code, not in requirements.txt)

### Important Notes
- The `requirements.txt` contains `asyncio` which is unnecessary (built-in Python module)
- MongoDB connection requires `pymongo` to be installed separately
- Playwright browsers must be installed using `playwright install` command

## Error Handling Patterns

The codebase implements comprehensive error handling:
- Network timeouts during page navigation
- MongoDB duplicate key constraints
- Missing or malformed page elements
- Connection failures with graceful degradation
- Page structure changes (multiple CSS selector fallbacks)