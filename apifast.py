from fastapi import FastAPI, Query
from datetime import datetime
from enum import Enum
import json
import os
from typing import Optional, List, Union
from pydantic import BaseModel


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


class FeedbackRequest(BaseModel):
    tab_type: TabType
    is_positive: bool
    llm_response: Union[str, List[str]]  # Can be either string or list of strings
    text_feedback: Optional[str] = None
    user_id: str


@app.post("/feedback")
async def save_feedback(feedback: FeedbackRequest):
    try:
        feedback_entry = {
            "tab_type": feedback.tab_type,
            "user_id": feedback.user_id,
            "is_positive": feedback.is_positive,
            "llm_response": feedback.llm_response,  # This will handle both string and list
            "timestamp": datetime.now().isoformat()
        }

        if feedback.text_feedback and feedback.text_feedback.strip():
            feedback_entry["text_feedback"] = feedback.text_feedback

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


"""
Example feedback data format in JSON:

For RCA (list of responses):
{
    "tab_type": "rca",
    "is_positive": true,
    "llm_response": ["response1", "response2", "response3"],
    "text_feedback": "Optional feedback",
    "user_id": "user@example.com"
}

For other tabs (single response):
{
    "tab_type": "triage",
    "is_positive": true,
    "llm_response": "single response",
    "text_feedback": "Optional feedback",
    "user_id": "user@example.com"
}
"""
