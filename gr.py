@app.get(path="/api/UniversalSearch", tags=['search_data'])
def universal_search(
    query: str = Query(..., description="Universal search across Incidents, Events, and CI records"),
    limit: int = Query(default=10, description="Maximum number of results per type", ge=1),
    page: int = Query(default=1, description="Page number", ge=1),
    query_type: str = Query(default=None, description="Filter results by type: event_list, incident_list, or ci_list")
):
    # Define search columns for each type
    EVENT_SEARCH_COLUMNS = ['col1', 'col2']
    INCIDENT_SEARCH_COLUMNS = ['col3', 'col4']
    CI_SEARCH_COLUMNS = ['col5', 'col6']
    
    # Define output columns for each type
    EVENT_OUTPUT_COLUMNS = ['col1', 'col2', 'status', 'priority']
    INCIDENT_OUTPUT_COLUMNS = ['col3', 'col4', 'status', 'severity']
    CI_OUTPUT_COLUMNS = ['col5', 'col6', 'type', 'location']
    
    if not query:
        raise HTTPException(status_code=400, detail="No search query provided")
    
    # Validate query_type if provided
    valid_query_types = ['event_list', 'incident_list', 'ci_list']
    if query_type and query_type.lower() not in valid_query_types:
        raise HTTPException(status_code=400, detail=f"Invalid query_type. Must be one of: {', '.join(valid_query_types)}")
    
    try:
        def determine_search_type(search_query):
            if search_query.upper().startswith('IM'):
                return 'incident'
            elif search_query[0].isdigit():
                return 'event'
            else:
                return 'ci'

        def prepare_for_json(df, output_columns):
            df = df[output_columns].astype(str)
            return df.to_dict(orient="records")
        
        search_type = determine_search_type(query)
        start_idx = (page - 1) * limit
        
        search_metadata = {
            "search_query": query,
            "search_type": search_type,
            "query_type": query_type,
            "pagination": {
                "current_page": int(page),
                "items_per_page": int(limit),
                "start_index": int(start_idx)
            }
        }

        # Handle exact matches
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
            result = {}
            total_matches = 0
            
            # Determine which searches to perform based on query_type
            do_event_search = not query_type or query_type.lower() == 'event_list'
            do_incident_search = not query_type or query_type.lower() == 'incident_list'
            do_ci_search = not query_type or query_type.lower() == 'ci_list'
            
            if do_event_search:
                evt_mask = events_df[EVENT_SEARCH_COLUMNS].astype(str).apply(
                    lambda x: x.str.contains(query, case=False)).any(axis=1)
                evt_total = int(evt_mask.sum())
                evt_total_pages = int((evt_total + limit - 1) // limit)
                evt_results = events_df[evt_mask].iloc[start_idx:start_idx + limit]
                
                result["Event List"] = {
                    "total_matches": evt_total,
                    "current_page": int(page),
                    "total_pages": evt_total_pages,
                    "data": prepare_for_json(evt_results, EVENT_OUTPUT_COLUMNS)
                }
                total_matches += evt_total

            if do_incident_search:
                inc_mask = incidents_df[INCIDENT_SEARCH_COLUMNS].astype(str).apply(
                    lambda x: x.str.contains(query, case=False)).any(axis=1)
                inc_total = int(inc_mask.sum())
                inc_total_pages = int((inc_total + limit - 1) // limit)
                inc_results = incidents_df[inc_mask].iloc[start_idx:start_idx + limit]
                
                result["Incident List"] = {
                    "total_matches": inc_total,
                    "current_page": int(page),
                    "total_pages": inc_total_pages,
                    "data": prepare_for_json(inc_results, INCIDENT_OUTPUT_COLUMNS)
                }
                total_matches += inc_total

            if do_ci_search:
                ci_mask = ci_df[CI_SEARCH_COLUMNS].astype(str).apply(
                    lambda x: x.str.contains(query, case=False)).any(axis=1)
                ci_total = int(ci_mask.sum())
                ci_total_pages = int((ci_total + limit - 1) // limit)
                ci_results = ci_df[ci_mask].iloc[start_idx:start_idx + limit]
                
                result["CI List"] = {
                    "total_matches": ci_total,
                    "current_page": int(page),
                    "total_pages": ci_total_pages,
                    "data": prepare_for_json(ci_results, CI_OUTPUT_COLUMNS)
                }
                total_matches += ci_total
            
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
