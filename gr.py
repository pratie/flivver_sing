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
        start_idx = (page - 1) * limit
        
        search_metadata = {
            "search_query": query,
            "search_type": search_type,
            "input_pattern": query[:2].upper() if query[:2].upper() == 'IM' else query[:2],
            "pagination": {
                "current_page": page,
                "items_per_page": limit,
                "start_index": start_idx
            }
        }

        def optimized_search(df, columns, query_str, chunk_size=10000):
            """Memory efficient search function using chunking"""
            total_count = 0
            matching_chunks = []
            
            # Only get required columns
            df_subset = df[columns]
            
            # Process in chunks
            for chunk_start in range(0, len(df_subset), chunk_size):
                chunk_end = min(chunk_start + chunk_size, len(df_subset))
                chunk = df_subset.iloc[chunk_start:chunk_end]
                
                # Convert chunk to string and search
                chunk = chunk.astype(str)
                mask = chunk.apply(lambda x: x.str.contains(query_str, case=False)).any(axis=1)
                
                # Count matches in this chunk
                chunk_matches = mask.sum()
                total_count += chunk_matches
                
                # If this chunk contains our pagination range, keep it
                if chunk_matches > 0 and (
                    start_idx < (total_count) and 
                    (start_idx + limit) > (total_count - chunk_matches)
                ):
                    # Get the full row data for matches in this chunk
                    matching_rows = df.iloc[chunk_start:chunk_end][mask]
                    matching_chunks.append(matching_rows)
                
                # Early exit if we have enough results
                if total_count > (start_idx + limit) * 2:
                    break
            
            # Combine chunks and apply final pagination
            if matching_chunks:
                results = pd.concat(matching_chunks)
                results = results.iloc[max(0, start_idx - (total_count - len(results))):
                                    max(0, start_idx - (total_count - len(results))) + limit]
            else:
                results = pd.DataFrame()
                
            return results, total_count

        # Handle different search types
        if search_type == 'incident':
            filtered_df = incidents_df[incidents_df["NUMBERPRGN"] == query]
            if filtered_df.empty:
                raise HTTPException(status_code=404, detail="Incident not found")
            result = {
                "Incident List": {
                    "total_matches": 1,
                    "current_page": page,
                    "total_pages": 1,
                    "data": filtered_df.to_dict(orient="records")
                }
            }
            
        elif search_type == 'event':
            filtered_df = events_df[events_df["EVENT_ID"] == int(query)]
            if filtered_df.empty:
                raise HTTPException(status_code=404, detail="Event not found")
            result = {
                "Event List": {
                    "total_matches": 1,
                    "current_page": page,
                    "total_pages": 1,
                    "data": filtered_df.to_dict(orient="records")
                }
            }
            
        else:  # Free text search
            # Process events normally (since it's smaller)
            evt_df_subset = events_df[EVENT_SEARCH_COLUMNS].astype(str)
            evt_mask = evt_df_subset.apply(lambda x: x.str.contains(query, case=False)).any(axis=1)
            evt_total = evt_mask.sum()
            evt_results = events_df[evt_mask].iloc[start_idx:start_idx + limit]
            
            # Use optimized search for CI and incidents
            ci_results, ci_total = optimized_search(ci_df, CI_SEARCH_COLUMNS, query)
            inc_results, inc_total = optimized_search(incidents_df, INCIDENT_SEARCH_COLUMNS, query)
            
            # Calculate total pages
            ci_total_pages = (ci_total + limit - 1) // limit
            inc_total_pages = (inc_total + limit - 1) // limit
            evt_total_pages = (evt_total + limit - 1) // limit
            
            result = {
                "Event List": {
                    "total_matches": int(evt_total),
                    "current_page": page,
                    "total_pages": evt_total_pages,
                    "data": evt_results.to_dict(orient="records")
                },
                "Incident List": {
                    "total_matches": int(inc_total),
                    "current_page": page,
                    "total_pages": inc_total_pages,
                    "data": inc_results.to_dict(orient="records")
                },
                "CI List": {
                    "total_matches": int(ci_total),
                    "current_page": page,
                    "total_pages": ci_total_pages,
                    "data": ci_results.to_dict(orient="records")
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
