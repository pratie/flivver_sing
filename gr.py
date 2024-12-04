import pandas as pd
import psycopg2
import json
from typing import Dict, List
import time


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
    """Fetch data from table with all columns"""
    try:
        # First get all column names
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{table_name.split('.')[-1]}'
                AND table_schema = '{table_name.split('.')[0]}'
                ORDER BY ordinal_position
            """)
            columns = [row[0] for row in cursor.fetchall()]
            print(f"Found columns: {columns}")

            # Now fetch the data with all columns
            columns_str = ', '.join(columns)
            query = f"SELECT {columns_str} FROM {table_name}"
            
            chunk_size = 100000
            chunks = []
            
            with conn.cursor('large_data_cursor') as data_cursor:
                print(f"Executing query: {query}")
                data_cursor.execute(query)
                
                while True:
                    data = data_cursor.fetchmany(chunk_size)
                    if not data:
                        break
                    df_chunk = pd.DataFrame(data, columns=columns)
                    chunks.append(df_chunk)
                    print(f"Fetched chunk of {len(df_chunk)} rows")
                
                if chunks:
                    final_df = pd.concat(chunks, ignore_index=True)
                    print(f"Total rows fetched: {len(final_df)}")
                    return final_df
                else:
                    print("No data found")
                    return pd.DataFrame(columns=columns)
                    
    except Exception as e:
        print(f"Error fetching data from {table_name}: {e}")
        return pd.DataFrame()


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
            print(f"\nProcessing table: {table_name}")
            print("=" * 50)
            results[key] = fetch_table_data(table_name, conn)
            
            if not results[key].empty:
                print(f"\nTable {table_name} Summary:")
                print(f"Rows: {len(results[key])}")
                print(f"Columns: {list(results[key].columns)}")
                print("=" * 50)
            else:
                print(f"No data fetched from {table_name}")
                print("=" * 50)
                
    except Exception as e:
        print(f"Error in parse_all_tables: {e}")
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed")

    return results


if __name__ == "__main__":
    try:
        print("Connecting to database...")
        conn = connect_to_postgres({})
        print("Successfully connected to database")
        
        print("\nStarting to fetch tables...")
        dfs = parse_all_tables(conn)

        print("\nFinal Summary of All Tables:")
        print("=" * 50)
        for table_name, df in dfs.items():
            print(f"\nTable: {table_name}")
            print(f"Shape: {df.shape}")
            print(f"Columns: {list(df.columns)}")
            
    except Exception as e:
        print(f"\nError in main execution: {e}")
