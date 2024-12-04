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
    """Fetch large datasets in chunks using server-side cursor"""
    chunk_size = 100000  # Adjust this based on your system's memory
    start_time = time.time()
    
    try:
        # Use server-side cursor by naming it
        with conn.cursor(name='fetch_large_data') as cursor:
            print(f"\nStarting to fetch data from {table_name}")
            cursor.execute(f"SELECT * FROM {table_name}")
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            print(f"Found {len(columns)} columns: {columns}")
            
            # Initialize empty list to store chunks
            chunks = []
            total_rows = 0
            
            while True:
                print(f"Fetching chunk of {chunk_size:,} rows... (Total so far: {total_rows:,})")
                data = cursor.fetchmany(chunk_size)
                if not data:
                    break
                chunk_df = pd.DataFrame(data, columns=columns)
                chunks.append(chunk_df)
                total_rows += len(chunk_df)
                
                # Calculate and display progress metrics
                elapsed_time = time.time() - start_time
                rows_per_second = total_rows / elapsed_time if elapsed_time > 0 else 0
                print(f"Progress: {len(chunk_df):,} rows in chunk, "
                      f"{total_rows:,} total rows, "
                      f"{rows_per_second:.0f} rows/second")
            
            print(f"\nCombining {len(chunks)} chunks...")
            final_df = pd.concat(chunks, ignore_index=True)
            
            # Calculate final metrics
            total_time = time.time() - start_time
            total_rows = len(final_df)
            avg_rows_per_second = total_rows / total_time if total_time > 0 else 0
            
            print(f"\nFinal Statistics:")
            print(f"Total Rows: {total_rows:,}")
            print(f"Total Time: {total_time:.2f} seconds")
            print(f"Average Speed: {avg_rows_per_second:.0f} rows/second")
            print(f"Final Dataset Shape: {final_df.shape}")
            print(f"Memory Usage: {final_df.memory_usage().sum() / 1024 / 1024:.2f} MB")
            
            return final_df
            
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
                print(f"Rows: {len(results[key]):,}")
                print(f"Columns: {len(results[key].columns)}")
                print(f"Memory Usage: {results[key].memory_usage().sum() / 1024 / 1024:.2f} MB")
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
        # Connect to database
        print("Connecting to database...")
        conn = connect_to_postgres({})  # Empty dict since we're getting params from secrets
        print("Successfully connected to database")
        
        # Fetch all tables
        print("\nStarting to fetch tables...")
        dfs = parse_all_tables(conn)

        # Print summary of results
        print("\nFinal Summary of All Tables:")
        print("=" * 50)
        for table_name, df in dfs.items():
            print(f"\nTable: {table_name}")
            print(f"Shape: {df.shape}")
            print(f"Memory Usage: {df.memory_usage().sum() / 1024 / 1024:.2f} MB")
            print(f"Columns: {df.columns.tolist()}")
            
    except Exception as e:
        print(f"\nError in main execution: {e}")
