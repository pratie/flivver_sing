from langchain_chroma import Chroma
from llm import vox_triage_response, get_root_by_id_p, get_root_by_id_pt, get_note_by_id, get_stepstaken_by_id, new_triage
import warnings
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException, Query
from termcolor import colored
from langchain_community.embeddings.sentence_transformer import SentenceTransformerEmbeddings
from typing import Optional
import boto3
from botocore.exceptions import ClientError
from langchain.docstore.document import Document
import pandas as pd
import requests, json
import re

warnings.simplefilter(action="ignore", category=FutureWarning)
warnings.simplefilter(action="ignore", category=DeprecationWarning)

# Load dataframes
dc_events_df = pd.read_parquet("data/dc.parquet")
dc_events_df.fillna(" ", inplace=True)
print(dc_events_df.shape)

incidents_df = pd.read_parquet("data/incidents.parquet")
problems_tasks_df = pd.read_parquet("data/problem_tasks.parquet")
problems_df = pd.read_parquet("data/problems.parquet")
ci_df = pd.read_parquet("data/ci.parquet")
rfc_df = pd.read_parquet("data/rfc.parquet")

# Create embedding function
embedding_function = SentenceTransformerEmbeddings(model_name="all-MiniLM-L6-v2")

# Initialize Chroma databases
db_events = Chroma(persist_directory="data/chroma_db_events", embedding_function=embedding_function)
ic_db1 = Chroma(persist_directory="data/chroma_db_icb", embedding_function=embedding_function)
p_db = Chroma(persist_directory="data/chroma_db_p2", embedding_function=embedding_function)
pt_db = Chroma(persist_directory="data/chroma_db_pt", embedding_function=embedding_function)
rfc_db = Chroma(persist_directory="data/chroma_rcb", embedding_function=embedding_function)

# FastAPI app setup
app = FastAPI()

origins = ["http://localhost:3000"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_secret():
    secret_name = "dev/genai/testapp"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']
    return secret

@app.get("/login")
def check_email_password(email: str, password: str):
    secret = get_secret()
    secret_dict = json.loads(secret)

    if email in secret_dict:
        return secret_dict[email] == password
    else:
        return False

@app.get("/api")
def read_root(event_id: int, input_query: str, site: Optional[str] = None, type: Optional[str] = None, network_name: Optional[str] = None):
    data = trigae(event_id)
    res_traige, _ = new_triage(str(data))

    # Extract keywords from query
    keywords = extract_keywords(input_query)

    # Create the where_document condition dynamically
    where_conditions = [{"$contains": keyword} for keyword in keywords]
    if site:
        where_conditions.append({"LOCATION": site})
    if type:
        where_conditions.append({"TYPE": type})
    if network_name:
        where_conditions.append({"NETWORK_NAME": network_name})
    where_document = {"$or": where_conditions}

    similar_steps_taken, similar_IC = db_ic(input_query, where_document=where_document)
    res_rfc, similar_RFC = db_rfc(input_query, where_document=where_document)
    steps_to_be_taken, similar_event = db(input_query, k=3, where_document=where_document)
    res, similar_problem = db_p(input_query, where_document=where_document)
    res_pt, similar_pt = db_pt(input_query, where_document=where_document)

    final_text = steps_to_be_taken + similar_steps_taken + res + res_pt
    suggested_steps = generate_suggested_steps_i(input_query, final_text)

    return {
        "gcc_notes": steps_to_be_taken,
        "incident_steps": similar_steps_taken,
        "rfc_steps": res_rfc,
        "problem_data": res,
        "problem_task_data": res_pt,
        "overall_runbook": suggested_steps,
        "similar_events": similar_event,
        "similar_incidents": similar_IC,
        "similar_rfc": similar_RFC,
        "similar_problems": similar_problem,
        "similar_pt": similar_pt,
        "triage_res": res_traige
    }

@app.get("/dashboard")
def dash(page: int = Query(1, ge=1), per_page: int = Query(10, ge=1)):
    df = dc_events_df.copy().drop_duplicates(subset=["event_id"])
    start = (page - 1) * per_page
    end = start + per_page
    result = df[start:end].to_dict(orient='records')
    return result

@app.get("/get_event_byid")
def get_event_by_id(id: int):
    df = dc_events_df.copy()
    df = df[df["event_id"] == id]

    if df.empty:
        raise HTTPException(status_code=404, detail="Event not found")

    result = df.to_dict(orient="records")[0]
    return result

# Helper functions
def trigae(input_id):
    match_in_a = dc_events_df[dc_events_df['event_id'] == input_id]

    if not match_in_a.empty:
        id_v1 = match_in_a['config_item_id']
        data_in_b = ci_df[ci_df['LOGICAL_NAME'] == id_v1]
        if not data_in_b.empty:
            return data_in_b.iloc[0].to_dict()
    return {}

def extract_keywords(query):
    # Extract words in all caps (3 or more letters)
    caps_words = re.findall(r'\b[A-Z]{3,}\b', query)
    # Extract words within parentheses
    words_in_parentheses = re.findall(r'\(([^)]+)\)', query)
    # Combine and remove duplicates
    keywords = list(set(caps_words + words_in_parentheses))
    return keywords

def generate_suggested_steps(query_text, similar_gccnotes):
    prompt = f"""Given the following query and similar resolved tickets, suggest steps to resolve the issue:

    Query: {query_text}

    {similar_gccnotes}

    Suggested steps to resolve the query:"""
    response, _ = vox_triage_response(prompt)
    return response

def generate_suggested_steps_i(query_text, similar_stepstaken):
    prompt = f"""Given the following query and similar resolved tickets, what could be the root cause issue and suggest steps to resolve the issue
    Query: {query_text}

    {similar_stepstaken}

    ROOT CAUSE AND Suggested steps to resolve the query:"""
    response, _ = vox_triage_response(prompt)
    return response

# Database search functions
def db(input_query, k=3, where_document=None):
    similar_gccnotes = []
    if where_document:
        similar_tickets = db_events.similarity_search(input_query, where_document=where_document, k=k)
    else:
        similar_tickets = db_events.similarity_search_with_relevance_scores(input_query, k=k)
    
    for ticket, score in similar_tickets:
        id = ticket.metadata['event_id']
        txt = get_note_by_id(dc_events_df, id)
        if not all(part == "nan" for part in txt.split()):
            similar_gccnotes.append(txt)
    suggested_steps = generate_suggested_steps(input_query, similar_gccnotes)
    return suggested_steps, similar_tickets

def db_ic(input_query, k=3, where_document=None):
    similar_stepstaken = []
    if where_document:
        docs = ic_db1.similarity_search(input_query, where_document=where_document, k=k)
    else:
        docs = ic_db1.similarity_search_with_relevance_scores(input_query, k)
    for ticket, score in docs:
        ic_id = ticket.metadata['NUMBERPRGN']
        text = get_stepstaken_by_id(incidents_df, ic_id)
        if not all(part == "nan" for part in text.split()):
            similar_stepstaken.append(text)
    suggested_steps = generate_suggested_steps_i(input_query, similar_stepstaken)
    return suggested_steps, docs

def db_ic_site(input_query, site, k=3):
    similar_stepstaken = []
    docs = ic_db1.similarity_search_with_relevance_scores(input_query, k, filter={"LOCATION": site})
    for ticket, score in docs:
        ic_id = ticket.metadata['NUMBERPRGN']
        text = get_stepstaken_by_id(incidents_df, ic_id)
        if not all(part == "nan" for part in text.split()):
            similar_stepstaken.append(text)
    suggested_steps = generate_suggested_steps_i(input_query, similar_stepstaken)
    return suggested_steps, docs

def db_rfc(input_query, k=3, where_document=None):
    similar_stepstaken = []
    if where_document:
        docs = rfc_db.similarity_search(input_query, where_document=where_document, k=k)
    else:
        docs = rfc_db.similarity_search_with_relevance_scores(input_query, k)
    for ticket, score in docs:
        ic_id = ticket.metadata['NUMBERPRGN']
        text = get_stepstaken_by_id(incidents_df, ic_id)
        if not all(part == "nan" for part in text.split()):
            similar_stepstaken.append(text)
    suggested_steps = generate_suggested_steps_i(input_query, similar_stepstaken)
    return suggested_steps, docs

def db_rfc_site(input_query, site, k=3):
    similar_stepstaken = []
    docs = rfc_db.similarity_search_with_relevance_scores(input_query, k, filter={"LOCATION": site})
    for ticket, score in docs:
        ic_id = ticket.metadata['NUMBERPRGN']
        text = get_stepstaken_by_id(incidents_df, ic_id)
        if not all(part == "nan" for part in text.split()):
            similar_stepstaken.append(text)
    suggested_steps = generate_suggested_steps_i(input_query, similar_stepstaken)
    return suggested_steps, docs

def db_p(input_query, where_document=None):
    if where_document:
        docs = p_db.similarity_search(input_query, where_document=where_document)
    else:
        docs = p_db.similarity_search_with_relevance_scores(input_query)
    steps = []
    for ticket, score in docs:
        id = ticket.metadata['ID']
        text = get_root_by_id_p(problems_df, id)
        steps.append(text)
    suggested_steps = generate_suggested_steps(input_query, steps)
    return suggested_steps, docs

def db_p_site(input_query, site):
    docs = p_db.similarity_search_with_relevance_scores(input_query, filter={"LOCATION": site})
    steps = []
    for ticket, score in docs:
        id = ticket.metadata['ID']
        text = get_root_by_id_p(problems_df, id)
        steps.append(text)
    suggested_steps = generate_suggested_steps(input_query, steps)
    return suggested_steps, docs

def db_pt(input_query, where_document=None):
    if where_document:
        docs = pt_db.similarity_search(input_query, where_document=where_document)
    else:
        docs = pt_db.similarity_search_with_relevance_scores(input_query)
    steps = []
    for ticket, score in docs:
        id = ticket.metadata['ID']
        text = get_root_by_id_pt(problems_tasks_df, id)
        steps.append(text)
    suggested_steps = generate_suggested_steps(input_query, steps)
    return suggested_steps, docs
