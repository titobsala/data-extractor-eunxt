import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import logging
from pathlib import Path

class SectorExtractor:
    def __init__(self):
        self.setup_logging()
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
        self.request_delay = 0.5  # Reduced delay as the endpoint is more specific
        self.retry_attempts = 3
        self.retry_delay = 2

    def setup_logging(self):
        log_dir = Path('./logs')
        log_dir.mkdir(exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_dir / 'sector_extractor.log', mode='w'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def get_sector(self, isin, mic):
        if pd.isna(isin) or pd.isna(mic):
            return "Invalid ISIN or MIC"

        url = f"https://live.euronext.com/en/ajax/getFactsheetInfoBlock/STOCK/{isin}-{mic}/fs_icb_block"
        
        for attempt in range(self.retry_attempts):
            try:
                self.logger.info(f"Fetching data for {isin}-{mic}")
                response = self.session.get(url, timeout=10)
                
                if response.status_code == 404:
                    self.logger.warning(f"No data found for {isin}-{mic} (404).")
                    return "Not Found"
                    
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Find the <td> element with the text "Industry"
                industry_header = soup.find('td', string="Industry")
                
                if industry_header and industry_header.find_next_sibling('td'):
                    sector = industry_header.find_next_sibling('td').text.strip()
                    self.logger.info(f"Success! Found Sector: '{sector}' for {isin}-{mic}")
                    return sector
                else:
                    self.logger.warning(f"Could not find 'Industry' field for {isin}-{mic} in the response.")
                    return "Not Found"

            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request failed for {isin}-{mic}: {e}")
                if attempt < self.retry_attempts - 1:
                    self.logger.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    self.logger.error(f"All retries failed for {isin}-{mic}")
                    return "Error"
            time.sleep(self.request_delay)
        return "Error"

    def run(self):
        input_csv = "/home/tito-sala/Code/Exo/data-extractor-eunxt/Euronext_Equities_with_MIC.csv"
        output_csv = "/home/tito-sala/Code/Exo/data-extractor-eunxt/Euronext_Equities_with_MIC_and_Sector.csv"
        
        df = pd.read_csv(input_csv)
        
        df["Sector"] = df.apply(lambda row: self.get_sector(row['ISIN'], row['MIC']), axis=1)
        
        df.to_csv(output_csv, index=False)
        
        self.logger.info(f"Processing complete for sample. Output saved to {output_csv}")
        self.logger.info("To process all companies, the script needs to be modified to use the full dataframe.")

if __name__ == "__main__":
    extractor = SectorExtractor()
    extractor.run()