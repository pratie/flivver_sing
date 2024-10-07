import pandas as pd
from langchain_community.utilities import SQLDatabase
from sqlalchemy import create_engine
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

# Load your datasets
incidents_df = pd.read_csv('incidents.csv')
rfc_df = pd.read_csv('rfc.csv')
events_df = pd.read_csv('events.csv')
ci_df = pd.read_csv('ci_data.csv')

# Initialize the embedding model
model = SentenceTransformer('all-MiniLM-L6-v2')

# Function to create embeddings
def create_embeddings(df, text_column):
    embeddings = []
    for text in tqdm(df[text_column], desc=f"Creating embeddings for {text_column}"):
        embedding = model.encode(str(text))  # Convert to string to handle any non-string data
        embeddings.append(embedding.tolist())  # Convert numpy array to list for storage
    return embeddings

# Create embeddings for each dataset
incidents_df['embeddings'] = create_embeddings(incidents_df, 'BRIEF_DESCRIPTION')
rfc_df['embeddings'] = create_embeddings(rfc_df, 'BRIEF_DESCRIPTION')
events_df['embeddings'] = create_embeddings(events_df, 'event_title')
ci_df['embeddings'] = create_embeddings(ci_df, 'DESCRIPTION')

# Set up SQLite database
db_path = 'enterprise_data.db'
engine = create_engine(f'sqlite:///{db_path}')

# Store datasets in SQLite
incidents_df.to_sql("Incidents", engine, index=False, if_exists='replace')
rfc_df.to_sql("RFC", engine, index=False, if_exists='replace')
events_df.to_sql("Events", engine, index=False, if_exists='replace')
ci_df.to_sql("CI_Data", engine, index=False, if_exists='replace')

# Verify data storage
db = SQLDatabase.from_uri(f'sqlite:///{db_path}')

# Function to print sample data from each table
def print_sample_data(table_name):
    result = db.run(f"SELECT * FROM {table_name} LIMIT 5;")
    print(f"\nSample data from {table_name}:")
    print(result)

# Verify data in each table
print_sample_data("Incidents")
print_sample_data("RFC")
print_sample_data("Events")
print_sample_data("CI_Data")

print("\nDatabase creation and data storage complete.")
