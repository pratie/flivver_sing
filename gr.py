import pandas as pd
import psycopg2
from typing import Dict, List


def connect_to_postgres(db_params: Dict) -> psycopg2.extensions.connection:
    """Establish connection to PostgreSQL database"""
    try:
        conn = psycopg2.connect(**db_params)
        return conn
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


def parse_all_tables(db_params: Dict) -> Dict[str, pd.DataFrame]:
    """Parse all specified tables and return dictionary of DataFrames"""
    tables = {
        'events': 'dc1.events',
        'incidents': 'dc1sm_ro.incidents',
        'rfc': 'dc1sm_ro.rfc',
        'problems': 'dc1sm_ro.problems',
        'problem_tasks': 'dc1sm_ro.problem_tasks',
        'ci': 'itsm_owner.cis'
    }

    conn = connect_to_postgres(db_params)
    results = {}

    try:
        for key, table_name in tables.items():
            print(f"Fetching data from {table_name}...")
            results[key] = fetch_table_data(table_name, conn)
            print(f"Successfully fetched {len(results[key])} rows from {table_name}")
    finally:
        conn.close()

    return results


# Example usage:
if __name__ == "__main__":
    db_params = {
        'host': 'your_host',
        'database': 'your_database',
        'user': 'your_username',
        'password': 'your_password'
    }

    # Fetch all tables
    dfs = parse_all_tables(db_params)

    # Example analysis
    for table_name, df in dfs.items():
        print(f"\nTable: {table_name}")
        print(f"Shape: {df.shape}")
        print("Columns:", df.columns.tolist())
