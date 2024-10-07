def read_table_as_dataframe(table_name, engine):
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql_query(query, engine)
    print(f"\nSample data from {table_name}:")
    print(df.head())  # Print first 5 rows
    return df
