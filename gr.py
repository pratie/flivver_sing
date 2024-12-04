import pandas as pd
import psycopg2
import json
from typing import Dict, List
import time
from tqdm import tqdm
import psutil
import os
from datetime import datetime, timedelta


def fetch_table_data(table_name: str, conn: psycopg2.extensions.connection) -> pd.DataFrame:
    """Fetch data from table with all columns, showing progress and memory usage"""
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
                three_months_ago = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
                query = f"""
                    SELECT {columns_str} 
                    FROM {table_name}
                    WHERE update_time >= '{three_months_ago}'
                """
                print(f"Filtering incidents from: {three_months_ago}")
            else:
                query = f"SELECT {columns_str} FROM {table_name}"

            # Get total row count for progress bar
            count_query = query.replace(columns_str, 'COUNT(*)', 1)
            cursor.execute(count_query)
            total_rows = cursor.fetchone()[0]
            print(f"Total rows to fetch: {total_rows:,}")
            
            chunk_size = 100000
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
                        chunks.append(df_chunk)
                        
                        rows_processed += len(df_chunk)
                        pbar.update(len(df_chunk))
                        
                        # Show memory usage every chunk
                        current_memory = get_memory_usage()
                        memory_used = current_memory - initial_memory
                        
                        print(f"\nChunk processed:"
                              f"\n- Rows in chunk: {len(df_chunk):,}"
                              f"\n- Total rows: {rows_processed:,}"
                              f"\n- Memory usage: {memory_used:.2f} MB"
                              f"\n- Processing rate: {rows_processed/(time.time()-start_time):.0f} rows/sec")
                
                if chunks:
                    print("\nCombining chunks...")
                    final_df = pd.concat(chunks, ignore_index=True)
                    
                    # Calculate final metrics
                    total_time = time.time() - start_time
                    final_memory = get_memory_usage() - initial_memory
                    
                    print(f"\nFinal Statistics:"
                          f"\n- Total rows: {len(final_df):,}"
                          f"\n- Total columns: {len(final_df.columns)}"
                          f"\n- Processing time: {total_time:.2f} seconds"
                          f"\n- Average speed: {len(final_df)/total_time:.0f} rows/sec"
                          f"\n- Memory used: {final_memory:.2f} MB")
                    
                    return final_df
                else:
                    print("No data found")
                    return pd.DataFrame(columns=columns)
                    
    except Exception as e:
        print(f"Error fetching data from {table_name}: {e}")
        return pd.DataFrame()
