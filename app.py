def read_table_as_dataframe(table_name, engine):
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql_query(query, engine)
    print(f"\nSample data from {table_name}:")
    print(df.head())  # Print first 5 rows
    return df
merged_df = events_df.merge(ci_df[['LOGICAL_NAME', 'TYPE']], on='LOGICAL_NAME', how='left')




from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
import ast
from datetime import datetime
import logging

# Initialize FastAPI application
app = FastAPI()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the input model
class QueryPayload(BaseModel):
    number: str  # Input number (incident or event)
    LOCATION: str = None  # Optional, for filtering
    Days_Since: int = None  # Optional, for filtering
    LOGICAL_NAME: str = None  # Optional, for filtering
    TYPE: str = None  # Optional, for filtering

# Assume sql_execute is defined and returns a DataFrame
def sql_execute(query, params=None):
    # Implement your database connection and query execution here
    # Example using SQLAlchemy:
    # from sqlalchemy import create_engine, text
    # engine = create_engine('your_database_connection_string')
    # with engine.connect() as connection:
    #     result = pd.read_sql_query(text(query), connection, params=params)
    # return result
    pass  # Replace with your implementation

@app.post("/semantic_search")
def semantic_search(payload: QueryPayload):
    try:
        # Validate that number is provided
        if not payload.number:
            raise HTTPException(status_code=400, detail="Number must be provided.")
        
        # Determine if the number is an incident number or event number
        if payload.number.startswith("IM"):  # It's an incident number
            query_input = "SELECT * FROM INCIDENTS WHERE NUMBERPRGN = :number"
            params_input = {'number': payload.number}
            df_input = sql_execute(query_input, params=params_input)
            
            if df_input.empty:
                raise HTTPException(status_code=404, detail="Incident not found.")
            
            # Get the description and embedding
            input_description = df_input['DESCRIPTION'].iloc[0]
            input_embedding_str = df_input['EMBEDDING'].iloc[0]
            input_embedding = ast.literal_eval(input_embedding_str)
            
            # Retrieve other incidents, events, RFCs
            query_other_incidents = "SELECT * FROM INCIDENTS WHERE NUMBERPRGN != :number"
            params_other_incidents = {'number': payload.number}
        else:  # It's an event number
            query_input = "SELECT * FROM EVENTS WHERE EVENTID = :number"
            params_input = {'number': payload.number}
            df_input = sql_execute(query_input, params=params_input)
            
            if df_input.empty:
                raise HTTPException(status_code=404, detail="Event not found.")
            
            # Get the description and embedding
            input_description = df_input['DESCRIPTION'].iloc[0]
            input_embedding_str = df_input['EMBEDDING'].iloc[0]
            input_embedding = ast.literal_eval(input_embedding_str)
            
            # Retrieve other incidents, events, RFCs
            query_other_incidents = "SELECT * FROM INCIDENTS"
            params_other_incidents = None
        
        # Continue retrieving events and RFCs
        query_events = "SELECT * FROM EVENTS"
        query_rfcs = "SELECT * FROM RFC"
        df_other_incidents = sql_execute(query_other_incidents, params=params_other_incidents)
        df_events = sql_execute(query_events)
        df_rfcs = sql_execute(query_rfcs)

        # Exclude the input record from the datasets
        if payload.number.startswith("IM"):
            df_other_incidents = df_other_incidents[df_other_incidents['NUMBERPRGN'] != payload.number]
        else:
            df_events = df_events[df_events['EVENTID'] != payload.number]

        # Apply filters to the datasets
        def apply_filters(df):
            if payload.LOCATION:
                df = df[df['LOCATION'] == payload.LOCATION]
            if payload.LOGICAL_NAME:
                df = df[df['LOGICAL_NAME'] == payload.LOGICAL_NAME]
            if payload.TYPE:
                df = df[df['TYPE'] == payload.TYPE]
            if payload.Days_Since is not None:
                current_date = datetime.now()
                df['DAYS_SINCE'] = (current_date - pd.to_datetime(df['DATE'])).dt.days
                df = df[df['DAYS_SINCE'] <= payload.Days_Since]
            return df
        
        df_other_incidents = apply_filters(df_other_incidents)
        df_events = apply_filters(df_events)
        df_rfcs = apply_filters(df_rfcs)

        # Compute similarities and prepare results
        similarity_threshold = 0.40  # 40% similarity threshold

        def compute_similarities(df, embedding_column, id_column, text_column):
            # Check if DataFrame is empty
            if df.empty:
                return pd.DataFrame(columns=[id_column, text_column, 'Similarity'])
            
            # Convert embeddings from string to list
            df[embedding_column] = df[embedding_column].apply(ast.literal_eval)
            embeddings = df[embedding_column].tolist()
            
            # Compute cosine similarity
            similarities = cosine_similarity([input_embedding], embeddings)[0]
            df['Similarity'] = similarities
            
            # Filter by similarity threshold
            df = df[df['Similarity'] >= similarity_threshold]
            
            # Sort by similarity
            df = df.sort_values(by='Similarity', ascending=False)
            
            # Convert Similarity to percentage
            df['Similarity'] = df['Similarity'] * 100
            
            # Select relevant columns
            df_result = df[[id_column, text_column, 'Similarity']]
            return df_result

        # Compute similarities for each dataset
        df_incidents_result = compute_similarities(
            df_other_incidents,
            embedding_column='EMBEDDING',
            id_column='NUMBERPRGN',
            text_column='DESCRIPTION'
        )
        
        df_events_result = compute_similarities(
            df_events,
            embedding_column='EMBEDDING',
            id_column='EVENTID',
            text_column='DESCRIPTION'
        )
        
        df_rfcs_result = compute_similarities(
            df_rfcs,
            embedding_column='EMBEDDING',
            id_column='RFC_NUMBER',  # Adjust if necessary
            text_column='DESCRIPTION'
        )

        # Convert results to JSON
        incidents_table = df_incidents_result.to_dict(orient='records')
        events_table = df_events_result.to_dict(orient='records')
        rfcs_table = df_rfcs_result.to_dict(orient='records')

        # Prepare the response
        response = {
            'input_description': input_description,
            'incidents_table': incidents_table,
            'events_table': events_table,
            'rfcs_table': rfcs_table
        }
        return response
    
    except HTTPException as e:
        raise e  # Re-raise HTTP exceptions to be handled by FastAPI
    except Exception as e:
        logger.error(f"Error during semantic search: {e}")
        raise HTTPException(status_code=500, detail=str(e))

