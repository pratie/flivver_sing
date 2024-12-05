class TabType(str, Enum):
    RCA = "rca"
    TRIAGE = "triage"
    EVENTS = "events"
    SUMMARY = "summary"
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
