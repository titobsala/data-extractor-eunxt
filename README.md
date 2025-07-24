# Euronext ESG Data Scraper

A Python web scraper to extract Environmental, Social, and Governance (ESG) indicators from Euronext's API for all companies listed in a CSV file.

## Project Overview

This scraper processes a CSV file containing company data (Name, ISIN, Symbol, Market, Currency, Turnover, MIC) and extracts ESG data from Euronext's API endpoints for each company.

### Data Sources Scraped:
- **Environmental indicators**: CO2 emissions, energy consumption, etc.
- **Social & governance indicators**: workforce diversity, board composition, etc.
- **EU taxonomy CSRD eligibility**: regulatory compliance data
- **ESG ratings**: third-party ratings and scores

### Output Structure:
```
output/
├── euronext_esg_data.json          # Main results (all successful companies)
├── failed_requests.json            # List of failed requests
└── html_files/                     # Original HTML files preserved
    ├── {ISIN}-{MIC}_esg_environmental_indicators.html
    ├── {ISIN}-{MIC}_esg_social_governance_indicators.html
    └── ...
```

## Quick Start

### 1. Install Dependencies
```bash
# Install Poetry if you haven't already
curl -sSL https://install.python-poetry.org | python3 -

# Install project dependencies
poetry install
```

### 2. Add Your Data File
Place your `Euronext_Equities_with_MIC.csv` file in the project root directory.

### 3. Run the Scraper

#### Test Mode (Recommended First):
```bash
# Enter Poetry environment
poetry shell

# Test with 5 companies
python src/euronext_scraper.py --test

# Test with 10 companies
python src/euronext_scraper.py --test --companies 10
```

#### Full Scraping:
```bash
# Scrape all companies (2,374 companies - takes ~63 minutes)
python src/euronext_scraper.py
```

#### Custom CSV file:
```bash
# Use different CSV file
python src/euronext_scraper.py --csv "path/to/your/file.csv"
```

## Expected Results

### Performance Metrics:
- **Rate limiting**: 2 requests per second
- **Success rate**: Typically 60-90% (many companies don't have ESG data)
- **Full scraping time**: ~63 minutes for 2,374 companies
- **Test scraping time**: ~30 seconds for 5 companies

### Output Files

#### `output/euronext_esg_data.json`
Main results file with all successfully scraped companies, containing structured ESG data with metadata about the scraping process.

#### `output/failed_requests.json`
List of all failed requests for debugging, including error messages and URLs.

#### `output/html_files/`
Original HTML files preserved for each successful request:
- `{ISIN}-{MIC}_esg_environmental_indicators.html`
- `{ISIN}-{MIC}_esg_social_governance_indicators.html`
- `{ISIN}-{MIC}_esg_eu_taxonomy_csrd_eligibility.html`
- `{ISIN}-{MIC}_esg_ratings.html`

## Troubleshooting

### Common Issues:

1. **No data found (404 errors)**: Normal - many companies don't publish ESG data
2. **Connection timeouts**: Increase `request_delay` in the code
3. **Rate limiting errors**: The script already includes appropriate delays
4. **Memory issues**: For very large datasets, consider processing in batches

### Log Files:
Check `logs/scraper.log` for detailed execution logs.

## Project Structure

```
.
├── README.md                    # This file
├── pyproject.toml              # Poetry dependencies
├── claude.md                   # Detailed implementation guide
├── src/
│   ├── __init__.py
│   └── euronext_scraper.py     # Main scraper class
├── output/                     # Generated results (created at runtime)
├── logs/                       # Log files (created at runtime)
└── Euronext_Equities_with_MIC.csv  # Your data file (add this)
```

## Dependencies

- **pandas**: CSV processing and data manipulation
- **requests**: HTTP requests to Euronext API
- **beautifulsoup4**: HTML parsing
- **lxml**: XML/HTML parser backend

## Author

Tito Barros Sala (titobsala@exo-team.com)

## License

This project is for legitimate data analysis and research purposes only. Please respect Euronext's terms of service and rate limits.