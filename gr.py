import pandas as pd
import psycopg2
import json
from typing import Dict, List
import time
from tqdm import tqdm
import psutil
import os
from datetime import datetime, timedelta
import gc


def get_memory_usage():
    """Get current memory usage of the process"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024  # in MB


def get_postgres_secrets():
    """
    Add your implementation for retrieving secrets
    This should return a JSON string containing database credentials
    """
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


def optimize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Optimize DataFrame memory usage by adjusting numeric data types"""
    for col in df.columns:
        # Convert integer columns to smallest possible int type
        if df[col].dtype == 'int64':
            if df[col].min() >= 0:
                if df[col].max() < 255:
                    df[col] = df[col].astype('uint8')
                elif df[col].max() < 65535:
                    df[col] = df[col].astype('uint16')
                elif df[col].max() < 4294967295:
                    df[col] = df[col].astype('uint32')
            else:
                if df[col].min() > -128 and df[col].max() < 127:
                    df[col] = df[col].astype('int8')
                elif df[col].min() > -32768 and df[col].max() < 32767:
                    df[col] = df[col].astype('int16')
                elif df[col].min() > -2147483648 and df[col].max() < 2147483647:
                    df[col] = df[col].astype('int32')
                    
        # Convert float columns to float32 if possible
        elif df[col].dtype == 'float64':
            df[col] = df[col].astype('float32')
    
    return df


def fetch_table_data(table_name: str, conn: psycopg2.extensions.connection) -> pd.DataFrame:
    """Fetch data from table with memory optimization"""
    start_time = time.time()
    initial_memory = get_memory_usage()
    
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
            print(f"\nFound {len(columns)} columns")

            # Construct query with time filter for incidents table
            columns_str = ', '.join(columns)
            if 'incidents' in table_name:
                seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
                query = f"""
                    SELECT {columns_str} 
                    FROM {table_name}
                    WHERE update_time >= '{seven_days_ago}'
                """
                print(f"Filtering incidents from: {seven_days_ago}")
            else:
                query = f"SELECT {columns_str} FROM {table_name}"

            # Get total row count
            count_query = query.replace(columns_str, 'COUNT(*)', 1)
            cursor.execute(count_query)
            total_rows = cursor.fetchone()[0]
            print(f"Total rows to fetch: {total_rows:,}")
            
            chunk_size = 50000
            chunks = []
            rows_processed = 0
            
            print("\nFetching data:")
            with conn.cursor('large_data_cursor') as data_cursor:
                data_cursor.execute(query)
                
                with tqdm(total=total_rows, unit='rows', unit_scale=True) as pbar:
                    while True:
                        data = data_cursor.fetchmany(chunk_size)
                        if not data:
                            break
                            
                        df_chunk = pd.DataFrame(data, columns=columns)
                        df_chunk = optimize_dataframe(df_chunk)
                        chunks.append(df_chunk)
                        
                        rows_processed += len(df_chunk)
                        pbar.update(len(df_chunk))
                        
                        current_memory = get_memory_usage()
                        memory_used = current_memory - initial_memory
                        elapsed_time = time.time() - start_time
                        
                        print(f"\nChunk processed:"
                              f"\n- Rows in chunk: {len(df_chunk):,}"
                              f"\n- Total rows: {rows_processed:,} of {total_rows:,}"
                              f"\n- Memory usage: {memory_used:.2f} MB"
                              f"\n- Processing rate: {rows_processed/elapsed_time:.0f} rows/sec")
                        
                        # Clear some memory if usage is high
                        if memory_used > 1000:  # If memory usage exceeds 1GB
                            gc.collect()
                
                if chunks:
                    print("\nCombining chunks...")
                    final_df = pd.concat(chunks, ignore_index=True)
                    
                    # Clear chunks to free memory
                    chunks.clear()
                    gc.collect()
                    
                    final_df = optimize_dataframe(final_df)
                    
                    total_time = time.time() - start_time
                    final_memory = get_memory_usage() - initial_memory
                    
                    print(f"\nFinal Statistics:"
                          f"\n- Total rows: {len(final_df):,}"
                          f"\n- Total columns: {len(final_df.columns)}"
                          f"\n- Processing time: {total_time:.2f} seconds"
                          f"\n- Memory used: {final_memory:.2f} MB")
                    
                    return final_df
                else:
                    print("No data found")
                    return pd.DataFrame(columns=columns)
                    
    except Exception as e:
        print(f"Error fetching data from {table_name}: {e}")
        return pd.DataFrame()


def parse_all_tables(conn: psycopg2.extensions.connection) -> Dict[str, pd.DataFrame]:
    """Parse all tables and return dictionary of DataFrames"""
    tables = {
        'events': 'dc1.events',
        'incidents': 'dc1sm_ro.incidents',
        'rfc': 'dc1sm_ro.rfc',
        'problems': 'dc1sm_ro.problems',
        'problem_tasks': 'dc1sm_ro.problem_tasks',
        'ci': 'itsm_owner.cis'
    }

    results = {}
    start_time = time.time()
    initial_memory = get_memory_usage()

    try:
        for key, table_name in tables.items():
            print(f"\nProcessing table: {table_name}")
            print("=" * 50)
            
            # Clear memory before each table
            gc.collect()
            
            results[key] = fetch_table_data(table_name, conn)
            
            if not results[key].empty:
                print(f"\nTable {table_name} Summary:")
                print(f"Rows: {len(results[key]):,}")
                print(f"Columns: {len(results[key].columns)}")
                print(f"Memory: {results[key].memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
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

    total_time = time.time() - start_time
    total_memory = get_memory_usage() - initial_memory
    print(f"\nTotal Processing Statistics:")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Total memory used: {total_memory:.2f} MB")

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
            print(f"Memory: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
            print(f"Columns: {list(df.columns)}")
            
    except Exception as e:
        print(f"\nError in main execution: {e}")
