#!/usr/bin/env python3
"""
JSON to CSV Converter for Euronext ESG Data
Converts the nested JSON structure to various CSV formats
"""

import json
import pandas as pd
import argparse
from pathlib import Path
from datetime import datetime

class ESGJsonToCsvConverter:
    def __init__(self, json_file_path):
        self.json_file_path = json_file_path
        self.data = None
        self.output_dir = Path('./csv_output')
        self.output_dir.mkdir(exist_ok=True)
        
    def load_json(self):
        """Load the JSON data"""
        print(f"Loading JSON file: {self.json_file_path}")
        with open(self.json_file_path, 'r', encoding='utf-8') as f:
            self.data = json.load(f)
        
        companies_count = len(self.data.get('companies', []))
        print(f"Loaded data for {companies_count} companies")
        return self.data
    
    def convert_companies_summary(self):
        """Convert to company-level summary CSV (one row per company)"""
        print("Converting to companies summary CSV...")
        
        companies_data = []
        
        for company_record in self.data.get('companies', []):
            company_info = company_record.get('company', {})
            esg_data = company_record.get('esg_data', {})
            
            row = {
                'company_name': company_info.get('name'),
                'isin': company_info.get('isin'),
                'mic': company_info.get('mic'),
                'symbol': company_info.get('symbol'),
                'market': company_info.get('market'),
                'currency': company_info.get('currency'),
                'scrape_timestamp': company_record.get('scrape_timestamp')
            }
            
            for category, category_data in esg_data.items():
                if isinstance(category_data, dict) and 'row_count' in category_data:
                    row[f'{category}_indicators_count'] = category_data['row_count']
                    
                    metadata = category_data.get('metadata', {})
                    if metadata:
                        row[f'{category}_source'] = metadata.get('source')
                        row[f'{category}_last_update'] = metadata.get('last_update')
            
            companies_data.append(row)
        
        df = pd.DataFrame(companies_data)
        output_file = self.output_dir / 'companies_summary.csv'
        df.to_csv(output_file, index=False)
        print(f"✅ Companies summary saved to: {output_file}")
        return output_file
    
    def convert_indicators_detailed(self):
        """Convert to detailed indicators CSV (one row per indicator per company)"""
        print("Converting to detailed indicators CSV...")
        
        indicators_data = []
        
        for company_record in self.data.get('companies', []):
            company_info = company_record.get('company', {})
            esg_data = company_record.get('esg_data', {})
            
            base_row = {
                'company_name': company_info.get('name'),
                'isin': company_info.get('isin'),
                'mic': company_info.get('mic'),
                'symbol': company_info.get('symbol'),
                'market': company_info.get('market'),
                'currency': company_info.get('currency'),
                'scrape_timestamp': company_record.get('scrape_timestamp')
            }
            
            for category, category_data in esg_data.items():
                if isinstance(category_data, dict) and 'data' in category_data:
                    metadata = category_data.get('metadata', {})
                    
                    for indicator_row in category_data['data']:
                        row = base_row.copy()
                        row.update({
                            'esg_category': category,
                            'indicator': indicator_row.get('Indicator'),
                            'unit': indicator_row.get('Unit'),
                            'value_2024': indicator_row.get('2024'),
                            'value_2023': indicator_row.get('2023'),
                            'value_2022': indicator_row.get('2022'),
                            'data_source': metadata.get('source'),
                            'data_last_update': metadata.get('last_update')
                        })
                        indicators_data.append(row)
        
        df = pd.DataFrame(indicators_data)
        output_file = self.output_dir / 'indicators_detailed.csv'
        df.to_csv(output_file, index=False)
        print(f"✅ Detailed indicators saved to: {output_file}")
        return output_file
    
    def convert_by_category(self):
        """Convert to separate CSV files for each ESG category"""
        print("Converting to category-specific CSV files...")
        
        output_files = []
        categories_data = {}
        
        for company_record in self.data.get('companies', []):
            company_info = company_record.get('company', {})
            esg_data = company_record.get('esg_data', {})
            
            base_row = {
                'company_name': company_info.get('name'),
                'isin': company_info.get('isin'),
                'mic': company_info.get('mic'),
                'symbol': company_info.get('symbol'),
                'market': company_info.get('market'),
                'currency': company_info.get('currency'),
                'scrape_timestamp': company_record.get('scrape_timestamp')
            }
            
            for category, category_data in esg_data.items():
                if isinstance(category_data, dict) and 'data' in category_data:
                    if category not in categories_data:
                        categories_data[category] = []
                    
                    metadata = category_data.get('metadata', {})
                    
                    for indicator_row in category_data['data']:
                        row = base_row.copy()
                        row.update({
                            'indicator': indicator_row.get('Indicator'),
                            'unit': indicator_row.get('Unit'),
                            'value_2024': indicator_row.get('2024'),
                            'value_2023': indicator_row.get('2023'),
                            'value_2022': indicator_row.get('2022'),
                            'data_source': metadata.get('source'),
                            'data_last_update': metadata.get('last_update')
                        })
                        categories_data[category].append(row)
        
        for category, data in categories_data.items():
            df = pd.DataFrame(data)
            output_file = self.output_dir / f'{category}.csv'
            df.to_csv(output_file, index=False)
            print(f"✅ {category} saved to: {output_file}")
            output_files.append(output_file)
        
        return output_files
    
    def convert_pivot_table(self):
        """Create pivot table format with indicators as columns"""
        print("Converting to pivot table format...")
        
        pivot_data = []
        
        for company_record in self.data.get('companies', []):
            company_info = company_record.get('company', {})
            esg_data = company_record.get('esg_data', {})
            
            row = {
                'company_name': company_info.get('name'),
                'isin': company_info.get('isin'),
                'mic': company_info.get('mic'),
                'symbol': company_info.get('symbol'),
                'market': company_info.get('market'),
                'currency': company_info.get('currency'),
                'scrape_timestamp': company_record.get('scrape_timestamp')
            }
            
            for category, category_data in esg_data.items():
                if isinstance(category_data, dict) and 'data' in category_data:
                    for indicator_row in category_data['data']:
                        indicator_name = indicator_row.get('Indicator', '')
                        unit = indicator_row.get('Unit', '')
                        
                        col_prefix = f"{category}_{indicator_name}".replace(' ', '_').replace('(', '').replace(')', '').replace('/', '_').replace('-', '_')
                        
                        row[f"{col_prefix}_2024"] = indicator_row.get('2024')
                        row[f"{col_prefix}_2023"] = indicator_row.get('2023')
                        row[f"{col_prefix}_2022"] = indicator_row.get('2022')
                        row[f"{col_prefix}_unit"] = unit
            
            pivot_data.append(row)
        
        df = pd.DataFrame(pivot_data)
        output_file = self.output_dir / 'esg_data_pivot.csv'
        df.to_csv(output_file, index=False)
        print(f"✅ Pivot table saved to: {output_file}")
        return output_file
    
    def convert_all_formats(self):
        """Convert to all available formats"""
        print("Converting to all CSV formats...")
        print("="*60)
        
        output_files = []
        
        output_files.append(self.convert_companies_summary())
        
        output_files.append(self.convert_indicators_detailed())
        
        output_files.extend(self.convert_by_category())
        
        output_files.append(self.convert_pivot_table())
        
        print("="*60)
        print(f"✅ All conversions complete! Generated {len(output_files)} CSV files in: {self.output_dir}")
        return output_files

def main():
    parser = argparse.ArgumentParser(description='Convert Euronext ESG JSON data to CSV formats')
    parser.add_argument('--input', '-i', default='output/euronext_esg_data.json', 
                      help='Input JSON file path (default: output/euronext_esg_data.json)')
    parser.add_argument('--format', '-f', choices=['summary', 'detailed', 'category', 'pivot', 'all'], 
                      default='all', help='Output format (default: all)')
    
    args = parser.parse_args()
    
    if not Path(args.input).exists():
        print(f"❌ Error: Input file '{args.input}' not found!")
        return 1
    
    converter = ESGJsonToCsvConverter(args.input)
    converter.load_json()
    
    try:
        if args.format == 'summary':
            converter.convert_companies_summary()
        elif args.format == 'detailed':
            converter.convert_indicators_detailed()
        elif args.format == 'category':
            converter.convert_by_category()
        elif args.format == 'pivot':
            converter.convert_pivot_table()
        elif args.format == 'all':
            converter.convert_all_formats()
            
        print("\n🎉 Conversion completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during conversion: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main()) 