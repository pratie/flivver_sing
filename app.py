import pandas as pd
from sqlalchemy import create_engine, text
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
import numpy as np
import json

# Load your datasets
incidents_df = pd.read_csv('incidents.csv')
# Load other dataframes as needed

# Initialize the embedding model
model = SentenceTransformer('all-MiniLM-L6-v2')

# Function to create embeddings
def create_embeddings(df, text_column):
    embeddings = []
    for text in tqdm(df[text_column], desc=f"Creating embeddings for {text_column}"):
        embedding = model.encode(str(text))
        embeddings.append(embedding)
    return embeddings

# Create embeddings for the incidents dataset
incidents_df['embeddings'] = create_embeddings(incidents_df, 'BRIEF_DESCRIPTION')

# Function to preprocess dataframe for SQLite storage
def preprocess_dataframe(df):
    for column in df.columns:
        if df[column].dtype == 'object':
            df[column] = df[column].astype(str)
        elif isinstance(df[column].iloc[0], np.ndarray):
            df[column] = df[column].apply(lambda x: json.dumps(x.tolist()))
    return df

# Preprocess the dataframe
incidents_df = preprocess_dataframe(incidents_df)

# Set up SQLite database
db_path = 'sqlite:///./src/sqldb_combine.db'
engine = create_engine(db_path)

# Store datasets in SQLite
incidents_df.to_sql("Incidents", engine, index=False, if_exists='replace', chunksize=1000)
print("Database creation and data storage complete.")

# Function to test data retrieval
def sql_execute(sql_query):
    with engine.connect() as conn:
        sql_response = conn.execute(text(f"{sql_query}"))
        rows = sql_response.fetchall()
        columns = sql_response.keys()
        df = pd.DataFrame.from_records(rows, columns=columns)
    return df

# Test data retrieval
test_query = "SELECT * FROM Incidents LIMIT 5"
result_df = sql_execute(test_query)
print("\nSample data retrieved from the database:")
print(result_df)

# Optional: Function to convert embeddings back to numpy arrays if needed
def convert_embeddings(df):
    if 'embeddings' in df.columns:
        df['embeddings'] = df['embeddings'].apply(lambda x: np.array(json.loads(x)))
    return df

# Example of converting embeddings back to numpy arrays
result_df = convert_embeddings(result_df)
print("\nEmbeddings converted back to numpy arrays:")
print(result_df['embeddings'].iloc[0])
