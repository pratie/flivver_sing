class TabType(str, Enum):
    RCA = "rca"
    TRIAGE = "triage"
    EVENTS = "events"
    SUMMARY = "summary"from fastapi import FastAPI, Query
from datetime import datetime
from enum import Enum
import json
import os
from typing import Optional

# Define all tab types exactly as in your system
class TabType(str, Enum):
    RCA = "rca"
    TRIAGE = "triage"
    EVENTS = "events"
    SUMMARY = "summary"
    LOCATION = "location"
    APPLICATION = "application"
    ASSIGNMENT = "assignment"
    DC1 = "dc1"
    DEVICE = "device"
    CONFIG = "configuration"

@app.post("/feedback")
async def save_feedback(
    tab_type: TabType = Query(..., description="Type of tab providing feedback"),
    is_positive: bool = Query(..., description="Whether the feedback is positive"),
    llm_response: str = Query(..., description="The LLM response being rated"),
    text_feedback: Optional[str] = Query(None, description="Optional text feedback"),
    user_id: str = Query(..., description="ID of the user providing feedback")
):
    """
    Save feedback for various tabs in the system.
    All parameters are expected as query parameters, not in the request body.
    """
    try:
        # Create feedback entry
        feedback_entry = {
            "tab_type": tab_type,
            "user_id": user_id,
            "is_positive": is_positive,
            "llm_response": llm_response,
            "timestamp": datetime.now().isoformat()
        }

        # Only add text_feedback if it has actual content
        if text_feedback is not None and text_feedback.strip():
            feedback_entry["text_feedback"] = text_feedback
        
        # Load existing feedback or create new list
        filename = "feedback.json"
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                try:
                    feedback_data = json.load(f)
                except json.JSONDecodeError:
                    feedback_data = []
        else:
            feedback_data = []

        # Add new feedback
        feedback_data.append(feedback_entry)

        # Save updated feedback
        with open(filename, 'w') as f:
            json.dump(feedback_data, f, indent=2)

        return {
            "status": "success",
            "data": feedback_entry
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }
    ANALYSIS = "analysis"

@app.post("/feedback")
async def save_feedback(
    tab_type: TabType,  # Will validate against the enum
    is_positive: bool,
    llm_response: str = Query(...),
    text_feedback: Optional[str] = Query(None),
    user_id: str = Query(...)
):
    # Create feedback entry
    feedback_entry = {
        "tab_type": tab_type,
        "user_id": user_id,
        "is_positive": is_positive,
        "llm_response": llm_response,
        "timestamp": datetime.now().isoformat()
    }

    # Only add text_feedback if it has actual content
    if text_feedback is not None and text_feedback.strip():
        feedback_entry["text_feedback"] = text_feedback

    # Load existing feedback or create a new list
    filename = f"{tab_type}_feedback.json"  # Separate file for each tab type
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            try:
                feedback_data = json.load(f)
            except json.JSONDecodeError:
                feedback_data = []
    else:
        feedback_data = []

    # Add new feedback
    feedback_data.append(feedback_entry)

    # Save updated feedback
    with open(filename, 'w') as f:
        json.dump(feedback_data, f, indent=2)

    return feedback_entry
