import sqlite3
import requests
import os
import json
from typing import List, Dict, Tuple

class ACSDatabase:
    def __init__(self, db_path: str = 'acs_variables.db'):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize the database with tables and indexes"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create variables table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS variables (
                id TEXT PRIMARY KEY,
                name TEXT,
                concept TEXT,
                group_name TEXT,
                year INTEGER,
                predicate_type TEXT,
                var_limit TEXT,
                attributes TEXT,
                var_values TEXT
            )
        ''')
        
        # Create search indexes for fast queries
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_name ON variables(name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_concept ON variables(concept)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_group ON variables(group_name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_year ON variables(year)')
        
        conn.commit()
        conn.close()
    
    def populate_from_api(self, year: int = 2023) -> int:
        """Fetch variables from Census API and populate database"""
        print(f"Fetching ACS variables for year {year}...")
        
        url = f"https://api.census.gov/data/{year}/acs/acs5/variables.json"
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        
        variables_data = response.json()
        variables = variables_data.get("variables", {})
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        count = 0
        for var_id, var_info in variables.items():
            cursor.execute('''
                INSERT OR REPLACE INTO variables 
                (id, name, concept, group_name, year, predicate_type, var_limit, attributes, var_values)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                var_id,
                var_info.get("label", ""),
                var_info.get("concept", ""),
                var_info.get("group", ""),
                year,
                var_info.get("predicateType", ""),
                var_info.get("limit", ""),
                json.dumps(var_info.get("attributes", {})),
                json.dumps(var_info.get("values", {}))
            ))
            count += 1
        
        conn.commit()
        conn.close()
        
        print(f"Stored {count} variables for year {year}")
        return count
    
    def search_variables(self, search_term: str, limit: int = 50) -> List[Tuple]:
        """Search variables by name, concept, or ID"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Split search term into individual words
        words = [word.strip() for word in search_term.split() if word.strip()]
        
        if not words:
            return []
        
        # Build query with AND conditions for each word
        where_conditions = []
        params = []
        
        for word in words:
            word_pattern = f'%{word}%'
            where_conditions.append('(name LIKE ? OR concept LIKE ? OR id LIKE ? OR group_name LIKE ?)')
            params.extend([word_pattern, word_pattern, word_pattern, word_pattern])
        
        # Join all conditions with AND
        where_clause = ' AND '.join(where_conditions)
        
        query = f'''
            SELECT id, name, concept, group_name, year 
            FROM variables 
            WHERE {where_clause}
            ORDER BY name
            LIMIT ?
        '''
        
        params.append(limit)
        cursor.execute(query, params)
        
        results = cursor.fetchall()
        conn.close()
        return results
    
    def get_variable_details(self, var_id: str) -> Dict:
        """Get detailed information for a specific variable"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM variables WHERE id = ?
        ''', (var_id,))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            result = dict(row)
            # Parse JSON fields
            if result.get('attributes'):
                result['attributes'] = json.loads(result['attributes'])
            if result.get('values'):
                result['values'] = json.loads(result['values'])
            return result
        return {}
    
    def get_variables_by_group(self, group_name: str, year: int = 2023) -> List[Tuple]:
        """Get all variables in a specific group"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, name, concept FROM variables 
            WHERE group_name = ? AND year = ?
            ORDER BY id
        ''', (group_name, year))
        
        results = cursor.fetchall()
        conn.close()
        return results
    
    def get_database_stats(self) -> Dict:
        """Get statistics about the database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Total variables
        cursor.execute('SELECT COUNT(*) FROM variables')
        total_vars = cursor.fetchone()[0]
        
        # Variables by year
        cursor.execute('SELECT year, COUNT(*) FROM variables GROUP BY year ORDER BY year')
        by_year = dict(cursor.fetchall())
        
        # Top groups
        cursor.execute('''
            SELECT group_name, COUNT(*) as count 
            FROM variables 
            WHERE group_name != '' 
            GROUP BY group_name 
            ORDER BY count DESC 
            LIMIT 10
        ''')
        top_groups = cursor.fetchall()
        
        conn.close()
        
        return {
            'total_variables': total_vars,
            'by_year': by_year,
            'top_groups': top_groups
        }

# Example usage
if __name__ == '__main__':
    # Create database instance
    db = ACSDatabase()
    
    # Populate with 2023 data
    db.populate_from_api(2023)
    
    # Search for variables
    results = db.search_variables('age')
    print(f"Found {len(results)} variables matching 'age':")
    for var_id, name, concept, group, year in results[:5]:
        print(f"  {var_id}: {name} ({concept})")
    
    # Get database stats
    stats = db.get_database_stats()
    print(f"\nDatabase contains {stats['total_variables']} variables")
    print(f"Years: {list(stats['by_year'].keys())}")