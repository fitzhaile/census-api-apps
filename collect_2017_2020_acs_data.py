#!/usr/bin/env python3
"""
Batch Collect ACS 5-Year Data for 2017-2020
Collects all variables from all tables for specified counties and stores in SQLite database.
Efficiently processes multiple years with smart API key management.
"""

import requests
import sqlite3
import time
import json
import logging
from datetime import datetime
from typing import List, Dict, Set
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('acs_2017_2020_collection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BatchACSCollector:
    def __init__(self, api_keys: List[str], db_path: str = "comprehensive_acs_data.db"):
        self.api_keys = api_keys
        self.current_key_index = 0
        self.db_path = db_path
        self.base_url = "https://api.census.gov/data"
        self.years = ["2017", "2018", "2019", "2020"]
        self.dataset = "acs/acs5"
        self.counties = {
            "Chatham": "051",
            "Liberty": "179", 
            "Effingham": "103",
            "Bryan": "029"
        }
        self.state_fips = "13"  # Georgia
        
        # Rate limiting
        self.requests_made = 0
        self.max_requests_per_day = 500
        self.request_delay = 1.0  # seconds between requests
        
        # Initialize database
        self.init_database()
        
    def init_database(self):
        """Initialize SQLite database with proper schema."""
        logger.info(f"Using existing database: {self.db_path}")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create tables if they don't exist (they should already exist)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS acs_tables (
                table_id TEXT PRIMARY KEY,
                table_name TEXT,
                table_description TEXT,
                variables_count INTEGER,
                discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS acs_variables (
                variable_id TEXT PRIMARY KEY,
                table_id TEXT,
                variable_name TEXT,
                variable_description TEXT,
                variable_type TEXT,
                discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (table_id) REFERENCES acs_tables (table_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS acs_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                variable_id TEXT,
                county_name TEXT,
                county_fips TEXT,
                year INTEGER,
                value REAL,
                margin_of_error REAL,
                data_type TEXT,
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (variable_id) REFERENCES acs_variables (variable_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS collection_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_id TEXT,
                county_name TEXT,
                status TEXT,
                variables_collected INTEGER,
                error_message TEXT,
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes for better performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_data_variable ON acs_data (variable_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_data_county ON acs_data (county_name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_data_year ON acs_data (year)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_variables_table ON acs_variables (table_id)')
        
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully")
    
    def get_current_api_key(self) -> str:
        """Get the current API key."""
        if self.current_key_index >= len(self.api_keys):
            raise Exception("All API keys exhausted!")
        return self.api_keys[self.current_key_index]
    
    def switch_to_next_api_key(self):
        """Switch to the next API key."""
        self.current_key_index += 1
        self.requests_made = 0  # Reset request counter
        if self.current_key_index < len(self.api_keys):
            logger.info(f"Switching to API key {self.current_key_index + 1}/{len(self.api_keys)}: {self.get_current_api_key()[:20]}...")
        else:
            logger.error("All API keys exhausted!")
    
    def make_request(self, url: str, params: Dict = None) -> Dict:
        """Make API request with rate limiting and error handling."""
        if self.requests_made >= self.max_requests_per_day:
            logger.warning("Daily API limit reached for current key, switching to next key...")
            self.switch_to_next_api_key()
        
        time.sleep(self.request_delay)
        self.requests_made += 1
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            logger.info(f"API Response received: {len(data)} rows")
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response text: {e.response.text}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response text: {e.response.text}")
            raise
    
    def discover_all_tables(self, year: str) -> List[str]:
        """Discover all available ACS 5-Year tables for a specific year."""
        logger.info(f"Discovering all ACS 5-Year tables for {year}...")
        
        # Get table list from Census API
        url = f"{self.base_url}/{year}/{self.dataset}/groups"
        data = self.make_request(url)
        
        tables = []
        # The API returns {"groups": [...]}, so we need to access the groups array
        if isinstance(data, dict) and 'groups' in data:
            for group in data['groups']:
                table_id = group.get('name', '')
                if table_id and table_id.startswith(('B', 'C', 'S')):
                    tables.append(table_id)
        else:
            # Fallback: if data is already an array
            for group in data:
                if isinstance(group, dict):
                    table_id = group.get('name', '')
                    if table_id and table_id.startswith(('B', 'C', 'S')):
                        tables.append(table_id)
        
        logger.info(f"Discovered {len(tables)} tables for {year}")
        return sorted(tables)
    
    def get_table_variables(self, year: str, table_id: str) -> List[Dict]:
        """Get all variables for a specific table."""
        logger.info(f"Getting variables for table {table_id} ({year})")
        
        url = f"{self.base_url}/{year}/{self.dataset}/groups/{table_id}"
        data = self.make_request(url)
        
        variables = []
        for var in data.get('variables', {}):
            var_info = data['variables'][var]
            # Skip metadata variables that aren't valid for data API
            if var in ['NAME', 'GEO_ID']:
                continue
            variables.append({
                'id': var,
                'name': var_info.get('label', ''),
                'description': var_info.get('concept', ''),
                'type': 'estimate' if var.endswith('E') else 'margin_of_error' if var.endswith('M') else 'other'
            })
        
        logger.info(f"Found {len(variables)} variables in table {table_id} ({year})")
        return variables
    
    def store_table_info(self, table_id: str, table_name: str, variables: List[Dict]):
        """Store table and variable information in database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Store table info (only if not already exists)
        cursor.execute('''
            INSERT OR IGNORE INTO acs_tables (table_id, table_name, table_description, variables_count)
            VALUES (?, ?, ?, ?)
        ''', (table_id, table_name, f"ACS 5-Year Table {table_id}", len(variables)))
        
        # Store variable info (only if not already exists)
        for var in variables:
            cursor.execute('''
                INSERT OR IGNORE INTO acs_variables 
                (variable_id, table_id, variable_name, variable_description, variable_type)
                VALUES (?, ?, ?, ?, ?)
            ''', (var['id'], table_id, var['name'], var['description'], var['type']))
        
        conn.commit()
        conn.close()
    
    def collect_county_data(self, year: str, table_id: str, variables: List[Dict]) -> Dict:
        """Collect data for all counties for a specific table."""
        logger.info(f"Collecting {year} data for table {table_id}")
        
        # Build variable list (both estimates and margins of error)
        var_list = [var['id'] for var in variables if var['type'] in ['estimate', 'margin_of_error']]
        
        if not var_list:
            logger.warning(f"No valid variables found for table {table_id}")
            return {}
        
        # Build county FIPS list (just the county part, not full FIPS)
        county_fips = list(self.counties.values())
        
        # Batch variables to avoid URL length limits (max ~50 variables per request)
        batch_size = 50
        all_data = {}
        
        for i in range(0, len(var_list), batch_size):
            batch_vars = var_list[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(var_list) + batch_size - 1)//batch_size} ({len(batch_vars)} variables)")
            
            # Make API request for this batch
            url = f"{self.base_url}/{year}/{self.dataset}"
            params = {
                'get': ','.join(batch_vars),
                'for': 'county:' + ','.join(county_fips),
                'in': f'state:{self.state_fips}',
                'key': self.get_current_api_key()
            }
            
            try:
                data = self.make_request(url, params)
                batch_data = self.parse_county_data(table_id, data, batch_vars)
                # Merge batch data into all_data
                for county, county_data in batch_data.items():
                    if county not in all_data:
                        all_data[county] = {}
                    all_data[county].update(county_data)
            except Exception as e:
                logger.error(f"Failed to collect batch data for table {table_id}: {e}")
                continue
        
        return all_data
    
    def parse_county_data(self, table_id: str, data: List, var_list: List[str]) -> Dict:
        """Parse API response and return structured data."""
        if not data or len(data) < 2:
            return {}
        
        headers = data[0]
        rows = data[1:]
        
        county_data = {}
        
        for row in rows:
            row_dict = dict(zip(headers, row))
            
            # Get county info
            county_fips = row_dict.get('county', '')
            county_name = None
            for name, fips in self.counties.items():
                if county_fips == fips:
                    county_name = name
                    break
            
            if not county_name:
                continue
            
            if county_name not in county_data:
                county_data[county_name] = {}
            
            # Store all variable values
            for var_id in var_list:
                value = row_dict.get(var_id)
                if value is not None and value != '':
                    try:
                        county_data[county_name][var_id] = float(value)
                    except (ValueError, TypeError):
                        county_data[county_name][var_id] = value
        
        return county_data
    
    def store_county_data(self, year: str, table_id: str, county_data: Dict):
        """Store collected county data in database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        total_variables = 0
        
        for county_name, variables in county_data.items():
            county_fips = self.counties[county_name]
            
            for var_id, value in variables.items():
                # Determine data type
                data_type = 'estimate' if var_id.endswith('E') else 'margin_of_error' if var_id.endswith('M') else 'other'
                
                cursor.execute('''
                    INSERT INTO acs_data 
                    (variable_id, county_name, county_fips, year, value, data_type)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (var_id, county_name, county_fips, int(year), value, data_type))
                
                total_variables += 1
        
        # Log collection
        cursor.execute('''
            INSERT INTO collection_log 
            (table_id, county_name, status, variables_collected)
            VALUES (?, ?, ?, ?)
        ''', (table_id, 'All Counties', 'Success', total_variables))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Stored {total_variables} data points for table {table_id} ({year})")
    
    def collect_year_data(self, year: str) -> Dict:
        """Collect all data for a specific year."""
        logger.info(f"Starting {year} ACS data collection...")
        start_time = datetime.now()
        
        year_stats = {
            'year': year,
            'tables_processed': 0,
            'successful_tables': 0,
            'failed_tables': 0,
            'data_points': 0,
            'variables': 0,
            'start_time': start_time
        }
        
        try:
            # Discover all tables
            tables = self.discover_all_tables(year)
            logger.info(f"Will collect data from {len(tables)} tables for {year}")
            
            for i, table_id in enumerate(tables, 1):
                logger.info(f"Processing table {i}/{len(tables)}: {table_id} ({year})")
                year_stats['tables_processed'] += 1
                
                try:
                    # Get variables for this table
                    variables = self.get_table_variables(year, table_id)
                    
                    if not variables:
                        logger.warning(f"No variables found for table {table_id}")
                        continue
                    
                    # Store table and variable info
                    self.store_table_info(table_id, f"Table {table_id}", variables)
                    
                    # Collect county data
                    county_data = self.collect_county_data(year, table_id, variables)
                    
                    if county_data:
                        # Store county data
                        self.store_county_data(year, table_id, county_data)
                        year_stats['successful_tables'] += 1
                        logger.info(f"Successfully collected data for table {table_id} ({year})")
                    else:
                        logger.warning(f"No data collected for table {table_id}")
                        year_stats['failed_tables'] += 1
                    
                    # Progress update
                    if i % 10 == 0:
                        elapsed = datetime.now() - start_time
                        logger.info(f"Progress: {i}/{len(tables)} tables processed. "
                                  f"Successful: {year_stats['successful_tables']}, Failed: {year_stats['failed_tables']}. "
                                  f"Elapsed: {elapsed}")
                    
                except Exception as e:
                    logger.error(f"Failed to process table {table_id} ({year}): {e}")
                    year_stats['failed_tables'] += 1
                    
                    # Log failure
                    conn = sqlite3.connect(self.db_path)
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT INTO collection_log 
                        (table_id, county_name, status, error_message)
                        VALUES (?, ?, ?, ?)
                    ''', (table_id, 'All Counties', 'Failed', str(e)))
                    conn.commit()
                    conn.close()
            
            # Calculate final stats
            end_time = datetime.now()
            year_stats['end_time'] = end_time
            year_stats['total_time'] = end_time - start_time
            
            # Get database counts for this year
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM acs_data WHERE year = ?", (int(year),))
            year_stats['data_points'] = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(DISTINCT variable_id) FROM acs_data WHERE year = ?", (int(year),))
            year_stats['variables'] = cursor.fetchone()[0]
            conn.close()
            
            logger.info("=" * 60)
            logger.info(f"{year} COLLECTION COMPLETE!")
            logger.info(f"Total tables processed: {year_stats['tables_processed']}")
            logger.info(f"Successful: {year_stats['successful_tables']}")
            logger.info(f"Failed: {year_stats['failed_tables']}")
            logger.info(f"Data points collected: {year_stats['data_points']}")
            logger.info(f"Variables collected: {year_stats['variables']}")
            logger.info(f"Total time: {year_stats['total_time']}")
            logger.info(f"API requests made: {self.requests_made}")
            logger.info("=" * 60)
            
            return year_stats
            
        except Exception as e:
            logger.error(f"{year} Collection failed: {e}")
            raise
    
    def run_batch_collection(self):
        """Run the complete batch data collection process for all years."""
        logger.info("Starting batch ACS data collection for 2017-2020...")
        overall_start_time = datetime.now()
        
        # Initialize with first API key
        logger.info(f"Using API key 1/{len(self.api_keys)}: {self.get_current_api_key()[:20]}...")
        
        all_year_stats = []
        
        try:
            for year in self.years:
                year_stats = self.collect_year_data(year)
                all_year_stats.append(year_stats)
                
                # Brief pause between years
                if year != self.years[-1]:  # Not the last year
                    logger.info("Brief pause before starting next year...")
                    time.sleep(2)
            
            # Final summary
            overall_end_time = datetime.now()
            overall_total_time = overall_end_time - overall_start_time
            
            logger.info("=" * 80)
            logger.info("BATCH COLLECTION COMPLETE!")
            logger.info("=" * 80)
            
            total_data_points = 0
            total_variables = 0
            
            for stats in all_year_stats:
                logger.info(f"{stats['year']}: {stats['data_points']} data points, {stats['variables']} variables")
                total_data_points += stats['data_points']
                total_variables += stats['variables']
            
            logger.info(f"TOTAL: {total_data_points} data points, {total_variables} variables")
            logger.info(f"Total time: {overall_total_time}")
            logger.info(f"API keys used: {self.current_key_index + 1}/{len(self.api_keys)}")
            logger.info("=" * 80)
            
            # Final database summary
            self.print_final_database_summary()
            
        except Exception as e:
            logger.error(f"Batch collection failed: {e}")
            raise
    
    def print_final_database_summary(self):
        """Print final summary of all collected data."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get counts by year
        cursor.execute("SELECT year, COUNT(*) as records, COUNT(DISTINCT variable_id) as variables FROM acs_data GROUP BY year ORDER BY year")
        year_data = cursor.fetchall()
        
        # Get total counts
        cursor.execute("SELECT COUNT(*) FROM acs_data")
        total_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT year) FROM acs_data")
        year_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT variable_id) FROM acs_data")
        total_variables = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT county_name) FROM acs_data")
        county_count = cursor.fetchone()[0]
        
        logger.info("FINAL DATABASE SUMMARY:")
        logger.info(f"Years in database: {year_count}")
        logger.info(f"Total records: {total_records}")
        logger.info(f"Total unique variables: {total_variables}")
        logger.info(f"Counties: {county_count}")
        logger.info("")
        logger.info("Data by year:")
        for year, records, variables in year_data:
            logger.info(f"  {year}: {records} records, {variables} variables")
        
        conn.close()


def main():
    """Main function to run the batch data collection."""
    # Read API keys from file
    with open('api_keys.txt', 'r') as f:
        api_keys = [line.strip() for line in f.readlines() if line.strip()]
    
    logger.info(f"Loaded {len(api_keys)} API keys from api_keys.txt")
    
    try:
        collector = BatchACSCollector(api_keys)
        collector.run_batch_collection()
        logger.info("Batch collection completed successfully!")
        
    except Exception as e:
        logger.error(f"Batch collection failed: {e}")
        raise


if __name__ == "__main__":
    main()

