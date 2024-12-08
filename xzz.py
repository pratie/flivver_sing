@app.get(path="/api/UniversalSearch", tags=['search_data'])
def universal_search(query: str = Query(..., description="Universal search across Incidents, Events, and CI records")):
    if not query:
        raise HTTPException(status_code=400, detail="No search query provided")
    
    try:
        # Function to determine search type based on input pattern
        def determine_search_type(search_query):
            if search_query.upper().startswith('IM'):
                return 'incident'
            elif search_query[0].isdigit():
                return 'event'
            else:
                return 'ci'
        
        search_type = determine_search_type(query)
        
        # Create search metadata
        search_metadata = {
            "search_query": query,
            "search_type": search_type,
            "input_pattern": query[:2].upper() if query[:2].upper() == 'IM' else query[:2],
            "timestamp": datetime.now().isoformat()
        }
        
        # Handle different search types
        if search_type == 'incident':
            df = incidents_df.copy(deep=True)
            # Exact match on NUMBERPRGN
            filtered_df = df[df["NUMBERPRGN"] == query]
            if filtered_df.empty:
                raise HTTPException(status_code=404, detail="Incident not found")
            result = filtered_df.to_dict(orient="records")[0]
            
        elif search_type == 'event':
            df = events_df.copy(deep=True)
            # Exact match on EVENT_ID
            filtered_df = df[df["EVENT_ID"] == query]
            if filtered_df.empty:
                raise HTTPException(status_code=404, detail="Event not found")
            result = filtered_df.to_dict(orient="records")[0]
            
        else:  # CI search - keep the broader search
            df = ci_df.copy(deep=True)
            df = df.astype(str)
            matched_records = df[df.apply(lambda row: row.astype(str).str.contains(query, case=False).any(), axis=1)]
            if matched_records.empty:
                raise HTTPException(status_code=404, detail="No CI records found")
            # For CI, we'll return all matches
            result = matched_records.to_dict(orient="records")
        
        # Prepare response
        response_data = {
            "metadata": search_metadata,
            "result": result,
            "total_matches": 1 if search_type in ['incident', 'event'] else len(result)
        }
        
        return JSONResponse(content=response_data)
    
    except HTTPException:
        raise
    except Exception as e:
        error_response = {
            "status_code": 400,
            "message": "Failed to return the filtered data",
            "error": repr(e),
            "search_query": query,
            "detected_type": determine_search_type(query)
        }
        return JSONResponse(content=error_response, status_code=400)
