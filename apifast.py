from fastapi import FastAPI, Query
from datetime import datetime
from enum import Enum
import json
import os
from typing import Optional

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
    try:
        feedback_entry = {
            "tab_type": tab_type,
            "user_id": user_id,
            "is_positive": is_positive,
            "llm_response": llm_response,
            "timestamp": datetime.now().isoformat()
        }

        if text_feedback is not None and text_feedback.strip():
            feedback_entry["text_feedback"] = text_feedback

        filename = "all_feedback.json"
        
        # Create file with empty list if it doesn't exist
        if not os.path.exists(filename):
            with open(filename, 'w') as f:
                json.dump([], f)

        # Read existing feedback
        with open(filename, 'r') as f:
            try:
                all_feedback = json.load(f)
            except json.JSONDecodeError:
                all_feedback = []

        # Add new feedback
        all_feedback.append(feedback_entry)

        # Save all feedback back to file
        with open(filename, 'w') as f:
            json.dump(all_feedback, f, indent=2)

        return {
            "status": "success",
            "data": feedback_entry
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "error_type": type(e).__name__
        }

# Optional: Add endpoint to get feedback statistics
@app.get("/feedback/stats")
async def get_feedback_stats(tab_type: Optional[TabType] = None):
    try:
        filename = "all_feedback.json"
        if not os.path.exists(filename):
            return {"message": "No feedback data available"}

        with open(filename, 'r') as f:
            all_feedback = json.load(f)

        if tab_type:
            # Filter feedback for specific tab
            feedback_data = [f for f in all_feedback if f["tab_type"] == tab_type]
        else:
            # Get stats for all tabs
            feedback_data = all_feedback

        total = len(feedback_data)
        if total == 0:
            return {"message": "No feedback found"}

        stats = {
            "total_feedback": total,
            "positive_feedback": sum(1 for f in feedback_data if f["is_positive"]),
            "feedback_with_text": sum(1 for f in feedback_data if "text_feedback" in f),
            "feedback_by_tab": {}
        }

        # Calculate per-tab statistics
        for tab in TabType:
            tab_feedback = [f for f in feedback_data if f["tab_type"] == tab]
            if tab_feedback:
                stats["feedback_by_tab"][tab] = {
                    "total": len(tab_feedback),
                    "positive": sum(1 for f in tab_feedback if f["is_positive"])
                }

        return stats

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }
