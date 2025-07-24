import pandas as pd
import requests
import json
import time
from pathlib import Path
from bs4 import BeautifulSoup, Tag
from datetime import datetime
import logging

class EuronextESGScraper:
    def __init__(self, test_mode=True):
        self.base_urls = {
            'indicators': 'https://live.euronext.com/en/ajax/getEsgIndicatorsBlock',
            'ratings': 'https://live.euronext.com/en/ajax/getEsgRatingsBlock'
        }

        self.endpoints = [
            'esg_environmental_indicators',
            'esg_social_governance_indicators',
            'esg_eu_taxonomy_csrd_eligibility'
        ]

        self.results = []
        self.failures = []
        self.request_delay = 0.5  
        self.retry_attempts = 2
        self.retry_delay = 1
        self.test_mode = test_mode


        self.setup_logging()

        self.create_directories()

        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })

    def setup_logging(self):
        """Setup logging configuration"""
        log_dir = Path('./logs')
        log_dir.mkdir(exist_ok=True)

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_dir / 'scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def create_directories(self):
        """Create output directories if they don't exist"""
        directories = ['./output', './output/html_files', './output/json_data']
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
        self.logger.info("Output directories created")

    def read_csv(self, file_path):
        """Read and process the CSV file"""
        try:
            df = pd.read_csv(file_path)

            valid_companies = df.dropna(subset=['ISIN', 'MIC'])

            self.logger.info(f"Found {len(valid_companies)} companies with valid ISIN and MIC codes")
            return valid_companies.to_dict('records')
        except Exception as e:
            self.logger.error(f"Error reading CSV: {e}")
            raise

    def make_request(self, url, retry_count=0):
        """Make HTTP request with retry logic"""
        try:
            self.logger.debug(f"Making request to: {url}")
            response = self.session.get(url, timeout=30)

            self.logger.debug(f"Response status: {response.status_code}")
            self.logger.debug(f"Response headers: {dict(response.headers)}")
            self.logger.debug(f"Response encoding: {response.encoding}")

            if response.status_code == 404:
                self.logger.debug("Received 404 - no data available")
                return None

            response.raise_for_status()

            content = response.text
            self.logger.debug(f"Response content length: {len(content)}")

            if len(content) > 0:
                non_printable = sum(1 for c in content[:20] if ord(c) < 32 or ord(c) > 126)
                if non_printable > 10:
                    self.logger.warning("Response appears to contain compressed/binary data - trying manual decompression")
                    try:
                        import brotli
                        import gzip

                        if 'br' in response.headers.get('content-encoding', '').lower():
                            content = brotli.decompress(response.content).decode('utf-8')
                            self.logger.debug("Successfully decompressed Brotli content")
                        elif 'gzip' in response.headers.get('content-encoding', '').lower():
                            content = gzip.decompress(response.content).decode('utf-8')
                            self.logger.debug("Successfully decompressed gzip content")
                        else:
                            try:
                                content = brotli.decompress(response.content).decode('utf-8')
                                self.logger.debug("Successfully decompressed as Brotli")
                            except:
                                content = gzip.decompress(response.content).decode('utf-8')
                                self.logger.debug("Successfully decompressed as gzip")

                    except ImportError:
                        self.logger.error("Brotli library not available, install with: pip install brotli")
                        return None
                    except Exception as decomp_error:
                        self.logger.error(f"Failed to decompress content: {decomp_error}")
                        return None

            content_preview = content[:200].replace('\n', ' ').replace('\r', ' ')
            self.logger.debug(f"Response preview: {content_preview}...")

            return content

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed: {str(e)}")
            if retry_count < self.retry_attempts:
                self.logger.warning(f"Retrying request ({retry_count + 1}/{self.retry_attempts}): {url}")
                time.sleep(self.retry_delay)
                return self.make_request(url, retry_count + 1)
            raise e

    def parse_html_table(self, html):
        """Parse HTML table to structured data using BeautifulSoup"""
        try:
            self.logger.debug(f"Parsing HTML content of length: {len(html)}")
            soup = BeautifulSoup(html, 'html.parser')
            tables = soup.find_all('table')

            if not tables:
                self.logger.debug("No tables found in HTML")
                if 'table' in html.lower() or 'indicator' in html.lower():
                    self.logger.warning("HTML seems to contain table data but no <table> tags found")
                return None

            self.logger.debug(f"Found {len(tables)} table(s)")

            table = None
            for i, t in enumerate(tables):
                if isinstance(t, Tag):
                    class_attr = t.get('class')
                    if class_attr and isinstance(class_attr, list):
                        class_str = ' '.join(class_attr)
                        self.logger.debug(f"Table {i} classes: {class_str}")
                        if 'table' in class_str:
                            table = t
                            self.logger.debug(f"Selected table {i} with classes: {class_str}")
                            break

            if not table:
                table = tables[0]
                self.logger.debug("Using first table as fallback")

            rows = table.find_all('tr')

            if not rows:
                self.logger.debug("No rows found in table")
                return None

            self.logger.debug(f"Found {len(rows)} rows in table")

            header_row = rows[0]
            header_cells = header_row.find_all('th')
            if not header_cells:
                header_cells = header_row.find_all('td')

            headers = [cell.get_text(strip=True) for cell in header_cells]

            if not headers:
                self.logger.debug("No headers found in table")
                return None

            self.logger.debug(f"Found headers: {headers}")

            data = []
            for row_idx, row in enumerate(rows[1:], 1):
                cells = row.find_all('td')
                self.logger.debug(f"Row {row_idx}: found {len(cells)} cells")

                if cells and len(cells) >= len(headers):
                    row_data = {}
                    for i, cell in enumerate(cells[:len(headers)]):
                        header = headers[i] if i < len(headers) else f'column_{i}'
                        cell_text = cell.get_text(strip=True)
                        row_data[header] = cell_text

                    if any(value.strip() for value in row_data.values()):
                        data.append(row_data)
                        self.logger.debug(f"Row {row_idx}: {row_data}")
                    else:
                        self.logger.debug(f"Row {row_idx}: skipped (empty data)")

            self.logger.debug(f"Extracted {len(data)} data rows")

            if len(data) == 0:
                self.logger.warning("Table found but no data rows extracted")

            metadata = self.extract_metadata(soup)

            result = {
                'headers': headers,
                'data': data,
                'row_count': len(data)
            }

            if metadata:
                result['metadata'] = metadata

            return result if data else None

        except Exception as e:
            self.logger.error(f"Error parsing HTML table: {e}")
            self.logger.debug(f"HTML content causing error (first 500 chars): {html[:500]}")
            return None

    def extract_metadata(self, soup):
        """Extract metadata from card footer"""
        try:
            footer = soup.find('div', class_='card-footer')
            if not footer:
                return None

            source_p = footer.find('p')
            if source_p:
                source_text = source_p.get_text(strip=True)
                self.logger.debug(f"Found metadata: {source_text}")

                metadata = {'raw_source': source_text}

                if 'Source:' in source_text:
                    parts = source_text.split('Source:')
                    if len(parts) > 1:
                        source_part = parts[1].strip()

                        if 'Last Update:' in source_part:
                            source_date_parts = source_part.split('Last Update:')
                            metadata['source'] = source_date_parts[0].strip().rstrip(' -')
                            if len(source_date_parts) > 1:
                                metadata['last_update'] = source_date_parts[1].strip()
                        else:
                            metadata['source'] = source_part

                return metadata

        except Exception as e:
            self.logger.debug(f"Error extracting metadata: {e}")

        return None

    def save_html_file(self, company, endpoint, html):
        """Save HTML file to disk"""
        file_name = f"{company['ISIN']}-{company['MIC']}_{endpoint}.html"
        file_path = Path('./output/html_files') / file_name

        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(html)
            self.logger.debug(f"Saved HTML file: {file_name}")
        except Exception as e:
            self.logger.error(f"Error saving HTML file {file_name}: {e}")

    def scrape_company(self, company, index, total):
        """Scrape ESG data for a single company"""
        company_data = {
            'company': {
                'name': company['Name'],
                'isin': company['ISIN'],
                'mic': company['MIC'],
                'symbol': company.get('Symbol'),
                'market': company.get('Market'),
                'currency': company.get('Currency')
            },
            'esg_data': {},
            'scrape_timestamp': datetime.now().isoformat()
        }

        identifier = f"{company['ISIN']}-{company['MIC']}"
        self.logger.info(f"[{index + 1}/{total}] Scraping {company['Name']} ({identifier})")

        has_any_data = False

        for endpoint in self.endpoints:
            url = f"{self.base_urls['indicators']}/{identifier}/{endpoint}"
            self.logger.info(f"  📊 Fetching {endpoint}...")
            self.logger.debug(f"  🔗 URL: {url}")

            try:
                html = self.make_request(url)

                if html is None:
                    self.logger.info(f"    ❌ No data found for {endpoint} (404 response)")
                    continue

                if len(html.strip()) == 0:
                    self.logger.warning(f"    ⚠️  Empty HTML response for {endpoint}")
                    continue

                self.save_html_file(company, endpoint, html)
                self.logger.debug(f"    💾 Saved HTML file for {endpoint}")

                parsed_data = self.parse_html_table(html)
                if parsed_data and parsed_data['row_count'] > 0:
                    company_data['esg_data'][endpoint] = parsed_data
                    has_any_data = True
                    self.logger.info(f"    ✅ Found {parsed_data['row_count']} rows for {endpoint}")
                else:
                    self.logger.warning(f"    ⚠️  No table data extracted for {endpoint} (HTML received but parsing failed)")
                    if any(keyword in html.lower() for keyword in ['indicator', 'table', 'tbody', 'thead']):
                        self.logger.warning(f"    🔍 HTML contains indicator-related content but parsing failed for {endpoint}")

            except Exception as e:
                error_msg = str(e)
                self.logger.error(f"    ❌ Failed to fetch {endpoint} for {company['Name']}: {error_msg}")
                self.failures.append({
                    'company': company['Name'],
                    'isin': company['ISIN'],
                    'mic': company['MIC'],
                    'endpoint': endpoint,
                    'error': error_msg,
                    'url': url
                })

            time.sleep(self.request_delay)

        ratings_url = f"{self.base_urls['ratings']}/{identifier}"
        self.logger.info("  📊 Fetching ESG ratings...")

        try:
            html = self.make_request(ratings_url)

            if html is not None:
                if len(html.strip()) == 0:
                    self.logger.warning("    ⚠️  Empty HTML response for ratings")
                else:
                    self.save_html_file(company, 'esg_ratings', html)
                    self.logger.debug("    💾 Saved HTML file for ratings")

                    parsed_data = self.parse_html_table(html)
                    if parsed_data and parsed_data['row_count'] > 0:
                        company_data['esg_data']['esg_ratings'] = parsed_data
                        has_any_data = True
                        self.logger.info(f"    ✅ Found {parsed_data['row_count']} rows for ratings")
                    else:
                        self.logger.warning("    ⚠️  No ratings data extracted (HTML received but parsing failed)")
                        if any(keyword in html.lower() for keyword in ['rating', 'table', 'tbody', 'thead']):
                            self.logger.warning("    🔍 HTML contains rating-related content but parsing failed")
            else:
                self.logger.info("    ❌ No ratings data found (404 response)")

        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"    ❌ Failed to fetch ratings for {company['Name']}: {error_msg}")
            self.failures.append({
                'company': company['Name'],
                'isin': company['ISIN'],
                'mic': company['MIC'],
                'endpoint': 'esg_ratings',
                'error': error_msg,
                'url': ratings_url
            })

        if has_any_data:
            self.results.append(company_data)
            self.logger.info(f"✅ Successfully scraped data for {company['Name']}")
        else:
            self.logger.warning(f"❌ No ESG data found for {company['Name']}")

        time.sleep(self.request_delay)

    def test_scrape(self, csv_file_path, num_companies=5):
        """Test scraping with a limited number of companies"""
        self.logger.info(f"🧪 Starting TEST scraping with {num_companies} companies...")

        companies = self.read_csv(csv_file_path)
        test_companies = companies[:num_companies]

        self.logger.info("Testing with companies:")
        for i, company in enumerate(test_companies):
            self.logger.info(f"  {i+1}. {company['Name']} ({company['ISIN']}-{company['MIC']})")

        for i, company in enumerate(test_companies):
            self.scrape_company(company, i, len(test_companies))

        self.save_results()
        self.print_summary()

    def scrape_all(self, csv_file_path):
        """Scrape all companies"""
        self.logger.info("🚀 Starting FULL ESG data scraping...")

        companies = self.read_csv(csv_file_path)
        total = len(companies)
        estimated_time = (total * 4 * self.request_delay) / 60

        self.logger.info(f"Processing {total} companies...")
        self.logger.info(f"Estimated time: {estimated_time:.1f} minutes")

        for i, company in enumerate(companies):
            self.scrape_company(company, i, total)

            if (i + 1) % 50 == 0:
                self.logger.info(f"Progress: {i + 1}/{total} companies completed")
                self.logger.info(f"Successful: {len(self.results)}, Failed: {len(self.failures)}")

        self.save_results()
        self.print_summary()

    def save_results(self):
        """Save all results to JSON files"""
        main_output = {
            'metadata': {
                'scrape_date': datetime.now().isoformat(),
                'total_companies_attempted': len(self.results) + len(self.failures),
                'successful_companies': len(self.results),
                'failed_companies': len(self.failures),
                'endpoints_per_company': 4,
                'test_mode': self.test_mode
            },
            'companies': self.results
        }

        with open('./output/euronext_esg_data.json', 'w', encoding='utf-8') as f:
            json.dump(main_output, f, indent=2, ensure_ascii=False)

        failures_output = {
            'metadata': {
                'scrape_date': datetime.now().isoformat(),
                'total_failures': len(self.failures)
            },
            'failed_requests': self.failures
        }

        with open('./output/failed_requests.json', 'w', encoding='utf-8') as f:
            json.dump(failures_output, f, indent=2, ensure_ascii=False)

        self.logger.info("✅ Results saved to output/ directory")

    def print_summary(self):
        """Print summary report"""
        total_attempted = len(self.results) + len(self.failures)
        success_rate = (len(self.results) / total_attempted * 100) if total_attempted > 0 else 0

        summary = {
            'total_companies': total_attempted,
            'successful': len(self.results),
            'failed': len(self.failures),
            'success_rate': f"{success_rate:.1f}%",
            'files_created': {
                'main_data': './output/euronext_esg_data.json',
                'failures': './output/failed_requests.json',
                'html_files': './output/html_files/'
            }
        }

        print("\n" + "="*50)
        print("📊 SCRAPING SUMMARY")
        print("="*50)
        print(json.dumps(summary, indent=2))
        print("="*50)


def main():
    import argparse

    """Main function to run the scraper"""
    parser = argparse.ArgumentParser(description='Euronext ESG Data Scraper')
    parser.add_argument('--test', action='store_true', help='Run in test mode with 5 companies')
    parser.add_argument('--companies', type=int, default=5, help='Number of companies for test mode')
    parser.add_argument('--csv', default='Euronext_Equities_with_MIC.csv', help='Path to CSV file')

    args = parser.parse_args()

    if args.test:
        scraper = EuronextESGScraper(test_mode=True)
        scraper.test_scrape(args.csv, num_companies=args.companies)
    else:
        scraper = EuronextESGScraper(test_mode=False)
        scraper.scrape_all(args.csv)


if __name__ == "__main__":
    main()
