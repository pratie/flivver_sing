from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

# Global variables
dataframes = None
processed_data = None
data = None

class CorrelationRequest(BaseModel):
    new_item: str

@app.on_event("startup")
async def startup_event():
    global dataframes, processed_data, data
    dataframes = load_s3_folder_to_dfs(BUCKET_NAME, FOLDERS)  # Load data from S3
    print(dataframes.keys())
    processed_data = process_dataframes(dataframes, rows=20)  # Process the dataframes
    data = create_all_embeddings(processed_data)  # Create embeddings for processed data

@app.post("/correlation")
async def analyze_item(request: CorrelationRequest):
    if data is None:
        raise HTTPException(status_code=500, detail="Data not loaded. Please try again later.")
    
    item = request.new_item
    item_embedding = create_embedding(item)  # Create embedding for the input item
    
    similar_items = find_similar_items_embedding(item_embedding, data)  # Find similar items using embeddings
    print(f"Similar items found: {len(similar_items)}")
    
    # Assuming bm25_models and gpt4_analysis are defined elsewhere
    similar_items = find_relevant_items_bm25(item, similar_items, bm25_models, data)  # Use BM25 for relevance
    analysis = gpt4_analysis(item, similar_items)  # Perform further analysis using GPT-4
    
    return analysis

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
