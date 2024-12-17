@app.get(path="/api/UniversalSearch", tags=['search_data'])
def universal_search(
    query: str = Query(..., description="Universal search across Incidents, Events, and CI records"),
    limit: int = Query(default=10, description="Maximum number of results per type", ge=1),
    page: int = Query(default=1, description="Page number", ge=1)
):
    # Define search columns for each type
    EVENT_SEARCH_COLUMNS = ['col1', 'col2']
    INCIDENT_SEARCH_COLUMNS = ['col3', 'col4']
    CI_SEARCH_COLUMNS = ['col5', 'col6']
    
    # Define output columns for each type (columns to return in response)
    EVENT_OUTPUT_COLUMNS = ['col1', 'col2', 'status', 'priority']  # Replace with your actual column names
    INCIDENT_OUTPUT_COLUMNS = ['col3', 'col4', 'status', 'severity']  # Replace with your actual column names
    CI_OUTPUT_COLUMNS = ['col5', 'col6', 'type', 'location']  # Replace with your actual column names
    
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

        def prepare_for_json(df, output_columns):
            """Convert dataframe to JSON-safe format with selected columns"""
            # Select only required columns and convert to string
            df = df[output_columns].astype(str)
            return df.to_dict(orient="records")
        
        search_type = determine_search_type(query)
        start_idx = (page - 1) * limit
        
        search_metadata = {
            "search_query": query,
            "search_type": search_type,
            "input_pattern": query[:2].upper() if query[:2].upper() == 'IM' else query[:2],
            "pagination": {
                "current_page": int(page),
                "items_per_page": int(limit),
                "start_index": int(start_idx)
            }
        }

        # Handle different search types
        if search_type == 'incident':
            filtered_df = incidents_df[incidents_df["NUMBERPRGN"] == query]
            if filtered_df.empty:
                raise HTTPException(status_code=404, detail="Incident not found")
            result = {
                "Incident List": {
                    "total_matches": 1,
                    "current_page": int(page),
                    "total_pages": 1,
                    "data": prepare_for_json(filtered_df, INCIDENT_OUTPUT_COLUMNS)
                }
            }
            
        elif search_type == 'event':
            filtered_df = events_df[events_df["EVENT_ID"] == int(query)]
            if filtered_df.empty:
                raise HTTPException(status_code=404, detail="Event not found")
            result = {
                "Event List": {
                    "total_matches": 1,
                    "current_page": int(page),
                    "total_pages": 1,
                    "data": prepare_for_json(filtered_df, EVENT_OUTPUT_COLUMNS)
                }
            }
            
        else:  # Free text search
            # CI search
            ci_mask = ci_df[CI_SEARCH_COLUMNS].astype(str).apply(
                lambda x: x.str.contains(query, case=False)).any(axis=1)
            ci_results = ci_df[ci_mask].iloc[start_idx:start_idx + limit]
            ci_total = int(ci_mask.sum())

            # Incidents search
            inc_mask = incidents_df[INCIDENT_SEARCH_COLUMNS].astype(str).apply(
                lambda x: x.str.contains(query, case=False)).any(axis=1)
            inc_results = incidents_df[inc_mask].iloc[start_idx:start_idx + limit]
            inc_total = int(inc_mask.sum())

            # Events search
            evt_mask = events_df[EVENT_SEARCH_COLUMNS].astype(str).apply(
                lambda x: x.str.contains(query, case=False)).any(axis=1)
            evt_results = events_df[evt_mask].iloc[start_idx:start_idx + limit]
            evt_total = int(evt_mask.sum())
            
            # Calculate total pages
            ci_total_pages = int((ci_total + limit - 1) // limit)
            inc_total_pages = int((inc_total + limit - 1) // limit)
            evt_total_pages = int((evt_total + limit - 1) // limit)
            
            result = {
                "Event List": {
                    "total_matches": evt_total,
                    "current_page": int(page),
                    "total_pages": evt_total_pages,
                    "data": prepare_for_json(evt_results, EVENT_OUTPUT_COLUMNS)
                },
                "Incident List": {
                    "total_matches": inc_total,
                    "current_page": int(page),
                    "total_pages": inc_total_pages,
                    "data": prepare_for_json(inc_results, INCIDENT_OUTPUT_COLUMNS)
                },
                "CI List": {
                    "total_matches": ci_total,
                    "current_page": int(page),
                    "total_pages": ci_total_pages,
                    "data": prepare_for_json(ci_results, CI_OUTPUT_COLUMNS)
                }
            }
            
            total_matches = ci_total + inc_total + evt_total
            if total_matches == 0:
                raise HTTPException(status_code=404, detail="No records found")
        
        response_data = {
            "metadata": search_metadata,
            "result": result,
            "total_matches": 1 if search_type in ['incident', 'event'] else int(total_matches)
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
