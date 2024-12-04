import pandas as pd
import psycopg2
import json
from typing import Dict, List


def get_postgres_secrets():
    """
    Add your implementation for retrieving secrets
    This should return a JSON string containing database credentials
    """
    # Example implementation - replace with your actual secrets retrieval method
    secrets = {
        'dbname': 'your_database',
        'username': 'your_username',
        'password': 'your_password',
        'host': 'your_host',
        'port': 'your_port'
    }
    return json.dumps(secrets)


def connect_to_postgres(db_params: Dict) -> psycopg2.extensions.connection:
    """Establish connection to PostgreSQL database"""
    result = get_postgres_secrets()
    result_dict = {}
    res = json.loads(result)
    result_dict.update(res)
    
    try:
        postgres_conn = psycopg2.connect(
            dbname=result_dict['dbname'],
            user=result_dict['username'],
            password=result_dict['password'],
            host=result_dict['host'],
            port=result_dict['port']
        )
        return postgres_conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise


def fetch_table_data(table_name: str, conn: psycopg2.extensions.connection) -> pd.DataFrame:
    """Fetch all data from specified table and return as DataFrame"""
    cursor = conn.cursor()
    try:
        # Get column names first
        cursor.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}'
        """)
        columns = [desc[0] for desc in cursor.fetchall()]

        # Fetch actual data
        cursor.execute(f"SELECT * FROM {table_name}")
        data = cursor.fetchall()

        # Create DataFrame
        df = pd.DataFrame(data, columns=columns)
        return df
    except Exception as e:
        print(f"Error fetching data from {table_name}: {e}")
        return pd.DataFrame()
    finally:
        cursor.close()


def parse_all_tables(conn: psycopg2.extensions.connection) -> Dict[str, pd.DataFrame]:
    """Parse all specified tables and return dictionary of DataFrames"""
    tables = {
        'events': 'dc1.events',
        'incidents': 'dc1sm_ro.incidents',
        'rfc': 'dc1sm_ro.rfc',
        'problems': 'dc1sm_ro.problems',
        'problem_tasks': 'dc1sm_ro.problem_tasks',
        'ci': 'itsm_owner.cis'
    }

    results = {}

    try:
        for key, table_name in tables.items():
            print(f"Fetching data from {table_name}...")
            results[key] = fetch_table_data(table_name, conn)
            print(f"Successfully fetched {len(results[key])} rows from {table_name}")
    finally:
        if conn:
            conn.close()

    return results


if __name__ == "__main__":
    # Connect to database
    conn = connect_to_postgres({})  # Empty dict since we're getting params from secrets
    
    # Fetch all tables
    dfs = parse_all_tables(conn)

    # Example analysis
    for table_name, df in dfs.items():
        print(f"\nTable: {table_name}")
        print(f"Shape: {df.shape}")
        print("Columns:", df.columns.tolist())
