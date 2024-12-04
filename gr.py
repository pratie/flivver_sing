# helper.py

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
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


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
    for col in df.columns:
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
        elif df[col].dtype == 'float64':
            df[col] = df[col].astype('float32')
    return df


def fetch_table_data(table_name: str, conn: psycopg2.extensions.connection) -> pd.DataFrame:
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{table_name.split('.')[-1]}'
                AND table_schema = '{table_name.split('.')[0]}'
                ORDER BY ordinal_position
            """)
            columns = [row[0] for row in cursor.fetchall()]

            columns_str = ', '.join(columns)
            if 'incidents' in table_name:
                seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
                query = f"""
                    SELECT {columns_str} 
                    FROM {table_name}
                    WHERE update_time >= '{seven_days_ago}'
                """
            else:
                query = f"SELECT {columns_str} FROM {table_name}"

            cursor.execute(query.replace(columns_str, 'COUNT(*)', 1))
            total_rows = cursor.fetchone()[0]

            chunk_size = 50000
            chunks = []
            rows_processed = 0
            
            with conn.cursor('large_data_cursor') as data_cursor:
                data_cursor.execute(query)
                
                with tqdm(total=total_rows, unit='rows', desc=f'Fetching {table_name}', leave=True) as pbar:
                    while True:
                        data = data_cursor.fetchmany(chunk_size)
                        if not data:
                            break
                        df_chunk = pd.DataFrame(data, columns=columns)
                        df_chunk = optimize_dataframe(df_chunk)
                        chunks.append(df_chunk)
                        rows_processed += len(df_chunk)
                        pbar.update(len(df_chunk))
                
                if chunks:
                    final_df = pd.concat(chunks, ignore_index=True)
                    chunks.clear()
                    gc.collect()
                    final_df = optimize_dataframe(final_df)
                    return final_df
                return pd.DataFrame(columns=columns)
                    
    except Exception as e:
        print(f"Error: {table_name} - {str(e)}")
        return pd.DataFrame()


def get_all_tables() -> Dict[str, pd.DataFrame]:
    """Main function to fetch all tables and return as dictionary of DataFrames"""
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
        print("Connecting to database...")
        conn = connect_to_postgres({})
        print("Starting data fetch...")
        
        for key, table_name in tables.items():
            print(f"Processing: {table_name}")
            results[key] = fetch_table_data(table_name, conn)
            if not results[key].empty:
                print(f"Completed: {table_name} - {results[key].shape}")
            gc.collect()
                
    finally:
        if conn:
            conn.close()

    return results
