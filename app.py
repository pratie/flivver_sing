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
import threading


def get_memory_usage():
    """Get current memory usage of the process"""
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
    """Optimize memory usage of DataFrame by downcasting numeric types"""
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


def fetch_table_data(table_name: str, conn: psycopg2.extensions.connection, last_update: datetime = None) -> pd.DataFrame:
    """Fetch data from specified table with optional incremental loading"""
    try:
        timestamp_columns = {
            'dc1.events': 'created_ts',
            'dc1sm_ro.incidents': 'update_time',
            'dc1sm_ro.rfc': 'update_time',
            'dc1sm_ro.problems': 'update_time',
            'dc1sm_ro.problem_tasks': 'update_time',
            'itsm_owner.cis': 'pfz_added_time'
        }
        
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{table_name.split('.')[-1]}'
                AND table_schema = '{table_name.split('.')[0]}'
                ORDER BY ordinal_position
            """)
            columns = [row[0] for row in cursor.fetchall()]

            if 'events' in table_name:
                columns = [col.lower() for col in columns]
            else:
                columns = [col.upper() for col in columns]

            columns_str = ', '.join(columns)
            
            if last_update and table_name in timestamp_columns:
                query = f"""
                    SELECT {columns_str} 
                    FROM {table_name}
                    WHERE {timestamp_columns[table_name]} >= '{last_update.strftime('%Y-%m-%d %H:%M:%S')}'
                """
                if 'incidents' in table_name:
                    query += " ORDER BY open_time DESC"
            else:
                if 'incidents' in table_name:
                    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
                    query = f"""
                        SELECT {columns_str} 
                        FROM {table_name}
                        WHERE update_time >= '{seven_days_ago}'
                        ORDER BY open_time DESC
                        LIMIT 200000
                    """
                else:
                    query = f"""
                        SELECT {columns_str} 
                        FROM {table_name}
                    """

            # Get row count
            count_query = query.replace(columns_str, 'COUNT(*)', 1)
            if 'ORDER BY' in count_query:
                count_query = count_query[:count_query.index('ORDER BY')]
            cursor.execute(count_query)
            total_rows = cursor.fetchone()[0]
            print(f"Total rows to fetch from {table_name}: {total_rows:,}")

            if total_rows == 0:
                return pd.DataFrame(columns=columns)

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
                        
                        if rows_processed % 500000 == 0:
                            current_memory = get_memory_usage()
                            print(f"\nProcessed {rows_processed:,} rows. Memory usage: {current_memory:.2f} MB")
                
                if chunks:
                    print("\nCombining chunks...")
                    final_df = pd.concat(chunks, ignore_index=True)
                    chunks.clear()
                    gc.collect()
                    final_df = optimize_dataframe(final_df)
                    return final_df
                return pd.DataFrame(columns=columns)
                    
    except Exception as e:
        print(f"Error: {table_name} - {str(e)}")
        return pd.DataFrame()


class PeriodicDataLoader:
    """Class to manage periodic data loading and updates"""
    def __init__(self, interval_minutes=15):
        self.interval_minutes = interval_minutes
        self.dataframes = {}
        self.last_update = None
        self.tables = {
            'events': 'dc1.events',
            'incidents': 'dc1sm_ro.incidents',
            'rfc': 'dc1sm_ro.rfc',
            'problems': 'dc1sm_ro.problems',
            'problem_tasks': 'dc1sm_ro.problem_tasks',
            'ci': 'itsm_owner.cis'
        }
        self._stop_event = threading.Event()
        self._update_thread = None

    def update_data(self):
        """Update all tables with new data"""
        try:
            print(f"\nUpdating data at {datetime.now()}")
            conn = connect_to_postgres({})
            
            for key, table_name in self.tables.items():
                print(f"\nProcessing: {table_name}")
                new_data = fetch_table_data(table_name, conn, self.last_update)
                
                if not new_data.empty:
                    if key in self.dataframes:
                        # Remove duplicates and combine with new data
                        self.dataframes[key] = pd.concat([new_data, self.dataframes[key]], ignore_index=True)
                        print(f"Updated {key} - total rows: {len(self.dataframes[key])}")
                    else:
                        self.dataframes[key] = new_data
                        print(f"Initialized {key} with {len(new_data)} rows")
                
            conn.close()
            self.last_update = datetime.now()
            print(f"Update completed at {self.last_update}")
            gc.collect()
            
        except Exception as e:
            print(f"Error updating data: {e}")

    def start_periodic_updates(self):
        """Start background thread for periodic updates"""
        def update_loop():
            while not self._stop_event.is_set():
                time.sleep(self.interval_minutes * 60)
                if not self._stop_event.is_set():
                    self.update_data()

        self._update_thread = threading.Thread(target=update_loop, daemon=True)
        self._update_thread.start()

    def stop_updates(self):
        """Stop periodic updates"""
        self._stop_event.set()
        if self._update_thread:
            self._update_thread.join()


def get_all_tables() -> Dict[str, pd.DataFrame]:
    """Main function to fetch all tables and return as dictionary of DataFrames with automatic updates"""
    loader = PeriodicDataLoader(interval_minutes=15)  # Change to 30 for 30-minute intervals
    loader.update_data()  # Initial load
    loader.start_periodic_updates()  # Start periodic updates
    return loader.dataframes
