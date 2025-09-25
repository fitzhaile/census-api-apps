#!/usr/bin/env python3
"""
Comprehensive ACS 5-Year Data Collector
Collects all variables from all tables for specified counties and stores in SQLite database.
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
        logging.FileHandler('acs_collection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ComprehensiveACSCollector:
    def __init__(self, api_key: str, db_path: str = "comprehensive_acs_data.db"):
        self.api_key = api_key
        self.db_path = db_path
        self.base_url = "https://api.census.gov/data"
        self.year = "2023"
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
        logger.info(f"Initializing database: {self.db_path}")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create tables
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
    
    def make_request(self, url: str, params: Dict = None) -> Dict:
        """Make API request with rate limiting and error handling."""
        if self.requests_made >= self.max_requests_per_day:
            logger.error("Daily API limit reached!")
            raise Exception("Daily API limit reached")
        
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
            logger.error(f"Response status: {response.status_code}")
            logger.error(f"Response text: {response.text}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            logger.error(f"Response text: {response.text}")
            raise
    
    def discover_all_tables(self) -> List[str]:
        """Discover all available ACS 5-Year tables."""
        logger.info("Discovering all ACS 5-Year tables...")
        
        # Get table list from Census API
        url = f"{self.base_url}/{self.year}/{self.dataset}/groups"
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
        
        logger.info(f"Discovered {len(tables)} tables")
        return sorted(tables)
    
    def get_table_variables(self, table_id: str) -> List[Dict]:
        """Get all variables for a specific table."""
        logger.info(f"Getting variables for table {table_id}")
        
        url = f"{self.base_url}/{self.year}/{self.dataset}/groups/{table_id}"
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
        
        logger.info(f"Found {len(variables)} variables in table {table_id}")
        return variables
    
    def store_table_info(self, table_id: str, table_name: str, variables: List[Dict]):
        """Store table and variable information in database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Store table info
        cursor.execute('''
            INSERT OR REPLACE INTO acs_tables (table_id, table_name, table_description, variables_count)
            VALUES (?, ?, ?, ?)
        ''', (table_id, table_name, f"ACS 5-Year Table {table_id}", len(variables)))
        
        # Store variable info
        for var in variables:
            cursor.execute('''
                INSERT OR REPLACE INTO acs_variables 
                (variable_id, table_id, variable_name, variable_description, variable_type)
                VALUES (?, ?, ?, ?, ?)
            ''', (var['id'], table_id, var['name'], var['description'], var['type']))
        
        conn.commit()
        conn.close()
    
    def collect_county_data(self, table_id: str, variables: List[Dict]) -> Dict:
        """Collect data for all counties for a specific table."""
        logger.info(f"Collecting data for table {table_id}")
        
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
            url = f"{self.base_url}/{self.year}/{self.dataset}"
            params = {
                'get': ','.join(batch_vars),
                'for': 'county:' + ','.join(county_fips),
                'in': f'state:{self.state_fips}',
                'key': self.api_key
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
    
    def store_county_data(self, table_id: str, county_data: Dict):
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
                ''', (var_id, county_name, county_fips, int(self.year), value, data_type))
                
                total_variables += 1
        
        # Log collection
        cursor.execute('''
            INSERT INTO collection_log 
            (table_id, county_name, status, variables_collected)
            VALUES (?, ?, ?, ?)
        ''', (table_id, 'All Counties', 'Success', total_variables))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Stored {total_variables} data points for table {table_id}")
    
    def run_comprehensive_collection(self):
        """Run the complete data collection process."""
        logger.info("Starting comprehensive ACS data collection...")
        start_time = datetime.now()
        
        try:
            # Discover all tables
            tables = self.discover_all_tables()
            logger.info(f"Will collect data from {len(tables)} tables")
            
            successful_tables = 0
            failed_tables = 0
            
            for i, table_id in enumerate(tables, 1):
                logger.info(f"Processing table {i}/{len(tables)}: {table_id}")
                
                try:
                    # Get variables for this table
                    variables = self.get_table_variables(table_id)
                    
                    if not variables:
                        logger.warning(f"No variables found for table {table_id}")
                        continue
                    
                    # Store table and variable info
                    self.store_table_info(table_id, f"Table {table_id}", variables)
                    
                    # Collect county data
                    county_data = self.collect_county_data(table_id, variables)
                    
                    if county_data:
                        # Store county data
                        self.store_county_data(table_id, county_data)
                        successful_tables += 1
                        logger.info(f"Successfully collected data for table {table_id}")
                    else:
                        logger.warning(f"No data collected for table {table_id}")
                        failed_tables += 1
                    
                    # Progress update
                    if i % 10 == 0:
                        elapsed = datetime.now() - start_time
                        logger.info(f"Progress: {i}/{len(tables)} tables processed. "
                                  f"Successful: {successful_tables}, Failed: {failed_tables}. "
                                  f"Elapsed: {elapsed}")
                    
                except Exception as e:
                    logger.error(f"Failed to process table {table_id}: {e}")
                    failed_tables += 1
                    
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
            
            # Final summary
            end_time = datetime.now()
            total_time = end_time - start_time
            
            logger.info("=" * 60)
            logger.info("COLLECTION COMPLETE!")
            logger.info(f"Total tables processed: {len(tables)}")
            logger.info(f"Successful: {successful_tables}")
            logger.info(f"Failed: {failed_tables}")
            logger.info(f"Total time: {total_time}")
            logger.info(f"API requests made: {self.requests_made}")
            logger.info("=" * 60)
            
            # Database summary
            self.print_database_summary()
            
        except Exception as e:
            logger.error(f"Collection failed: {e}")
            raise
    
    def print_database_summary(self):
        """Print summary of collected data."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get counts
        cursor.execute("SELECT COUNT(*) FROM acs_tables")
        table_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM acs_variables")
        variable_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM acs_data")
        data_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT county_name) FROM acs_data")
        county_count = cursor.fetchone()[0]
        
        logger.info("DATABASE SUMMARY:")
        logger.info(f"Tables: {table_count}")
        logger.info(f"Variables: {variable_count}")
        logger.info(f"Data points: {data_count}")
        logger.info(f"Counties: {county_count}")
        
        conn.close()


def main():
    """Main function to run the comprehensive data collection."""
    # You'll need to set your Census API key
    api_key = os.getenv('CENSUS_API_KEY')
    
    if not api_key:
        logger.error("Please set CENSUS_API_KEY environment variable")
        return
    
    collector = ComprehensiveACSCollector(api_key)
    collector.run_comprehensive_collection()


if __name__ == "__main__":
    main()
