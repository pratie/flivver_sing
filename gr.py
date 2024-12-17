@app.get(path="/api/UniversalSearch", tags=['search_data'])
def universal_search(
    query: str = Query(..., description="Universal search across Incidents, Events, and CI records"),
    limit: int = Query(default=10, description="Maximum number of results per type", ge=1)
):
    # Define search columns for each type
    EVENT_SEARCH_COLUMNS = ['col1', 'col2']
    INCIDENT_SEARCH_COLUMNS = ['col3', 'col4']
    CI_SEARCH_COLUMNS = ['col5', 'col6']
    
    if not query:
        raise HTTPException(status_code=400, detail="No search query provided")
    
    try:
        def determine_search_type(search_query):
            if search_query.upper().startswith('IM'):
                return 'incident'
            elif search_query[0].isdigit():
                return 'event'
            else:
                return 'ci'
        
        search_type = determine_search_type(query)
        
        search_metadata = {
            "search_query": query,
            "search_type": search_type,
            "input_pattern": query[:2].upper() if query[:2].upper() == 'IM' else query[:2],
            "limit_per_type": limit
        }
        
        # Handle different search types
        if search_type == 'incident':
            df = incidents_df.copy(deep=True)
            for col in df.select_dtypes(include=['datetime64[ns]']).columns:
                df[col] = df[col].astype(str)
            
            filtered_df = df[df["NUMBERPRGN"] == query]
            if filtered_df.empty:
                raise HTTPException(status_code=404, detail="Incident not found")
            
            result = {
                "Incident List": {
                    "total_matches": 1,
                    "data": filtered_df.to_dict(orient="records")
                }
            }
            
        elif search_type == 'event':
            df = events_df.copy(deep=True)
            for col in df.select_dtypes(include=['datetime64[ns]']).columns:
                df[col] = df[col].astype(str)
                
            filtered_df = df[df["EVENT_ID"] == int(query)]
            if filtered_df.empty:
                raise HTTPException(status_code=404, detail="Event not found")
            
            result = {
                "Event List": {
                    "total_matches": 1,
                    "data": filtered_df.to_dict(orient="records")
                }
            }
            
        else:  # Free text search across specific columns
            # CI search
            ci_df_copy = ci_df.copy(deep=True)
            ci_df_copy = ci_df_copy.astype(str)
            ci_matches = ci_df_copy[
                ci_df_copy[CI_SEARCH_COLUMNS].apply(lambda x: x.str.contains(query, case=False)).any(axis=1)
            ]
            
            # Incidents search
            inc_df_copy = incidents_df.copy(deep=True)
            inc_df_copy = inc_df_copy.astype(str)
            incident_matches = inc_df_copy[
                inc_df_copy[INCIDENT_SEARCH_COLUMNS].apply(lambda x: x.str.contains(query, case=False)).any(axis=1)
            ]
            
            # Events search
            evt_df_copy = events_df.copy(deep=True)
            evt_df_copy = evt_df_copy.astype(str)
            event_matches = evt_df_copy[
                evt_df_copy[EVENT_SEARCH_COLUMNS].apply(lambda x: x.str.contains(query, case=False)).any(axis=1)
            ]
            
            # Structure results with counts and limited data
            result = {
                "Event List": {
                    "total_matches": len(event_matches),
                    "data": event_matches.head(limit).to_dict(orient="records")
                },
                "Incident List": {
                    "total_matches": len(incident_matches),
                    "data": incident_matches.head(limit).to_dict(orient="records")
                },
                "CI List": {
                    "total_matches": len(ci_matches),
                    "data": ci_matches.head(limit).to_dict(orient="records")
                }
            }
            
            total_matches = len(ci_matches) + len(incident_matches) + len(event_matches)
            if total_matches == 0:
                raise HTTPException(status_code=404, detail="No records found")
        
        response_data = {
            "metadata": search_metadata,
            "result": result,
            "total_matches": 1 if search_type in ['incident', 'event'] else total_matches
        }
        
        return JSONResponse(content=response_data)
    
    except ValueError as ve:
        error_response = {
            "status_code": 400,
            "message": "Invalid event ID format",
            "error": "Event ID must be a valid integer",
            "search_query": query,
            "detected_type": determine_search_type(query)
        }
        return JSONResponse(content=error_response, status_code=400)
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
