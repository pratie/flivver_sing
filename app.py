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

            # Format column names based on table
            if 'events' in table_name:
                columns = [col.lower() for col in columns]
            else:
                columns = [col.upper() for col in columns]

            columns_str = ', '.join(columns)
            if 'incidents' in table_name:
                seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
                query = f"""
                    SELECT {columns_str} 
                    FROM {table_name}
                    WHERE update_time >= '{seven_days_ago}'
                    LIMIT 100000
                """
            else:
                query = f"""
                    SELECT {columns_str} 
                    FROM {table_name}
                    LIMIT 100000
                """

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
                        # Create DataFrame with formatted column names
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
                    # No need to rename columns here as they're already formatted
                    return final_df
                return pd.DataFrame(columns=columns)
                    
    except Exception as e:
        print(f"Error: {table_name} - {str(e)}")
        return pd.DataFrame()
