import os
import sys
import logging
import sqlite3
import nest_asyncio
import gradio as gr
from llama_index.core import GPTVectorStoreIndex, SimpleDirectoryReader, ServiceContext, StorageContext, load_index_from_storage
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.core import Settings, set_global_service_context
from llama_index.core.query_engine.router_query_engine import RouterQueryEngine
from llama_index.core.selectors.llm_selectors import LLMSingleSelector
from llama_index.core.tools.query_engine import QueryEngineTool
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.llms.openai import OpenAI
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.handlers = []
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
logger.addHandler(handler)

os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")

# Initialize Anthropic and HuggingFace models
llm = OpenAI(temperature=0.5, model_name="gpt-3.5-turbo")
embed_model = OpenAIEmbedding()
# Configure settings
chunk_size = 200
Settings.llm = llm
Settings.embed_model = embed_model
Settings.chunk_size = chunk_size

# Initialize SQLite database
def init_db():
    conn = sqlite3.connect('chat_history.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS chat_history (
            user_id TEXT,
            message TEXT,
            response TEXT
        )
    ''')
    conn.commit()
    conn.close()

init_db()

def construct_index(directory_path):
    docs = SimpleDirectoryReader(directory_path).load_data()
    index = GPTVectorStoreIndex.from_documents(docs, service_context=ServiceContext.from_defaults())
    index.storage_context.persist(persist_dir="indexes")
    return index

def save_chat_history(user_id, message, response):
    conn = sqlite3.connect('chat_history.db')
    c = conn.cursor()
    c.execute('INSERT INTO chat_history (user_id, message, response) VALUES (?, ?, ?)', (user_id, message, response))
    conn.commit()
    conn.close()

def get_chat_history(user_id):
    conn = sqlite3.connect('chat_history.db')
    c = conn.cursor()
    c.execute('SELECT message, response FROM chat_history WHERE user_id = ?', (user_id,))
    history = c.fetchall()
    conn.close()
    return history

def chatbot(user_id, input_text):
    storage_context = StorageContext.from_defaults(persist_dir="indexes")
    query_engine = load_index_from_storage(storage_context).as_query_engine()
    response = query_engine.query(input_text)
    save_chat_history(user_id, input_text, response.response)
    return response.response

def get_history(user_id):
    history = get_chat_history(user_id)
    formatted_history = "\n".join([f"User: {msg}\nBot: {resp}" for msg, resp in history])
    return formatted_history or "No history found for this user."

def chat_interface(user_id, input_text):
    if not input_text:
        return get_history(user_id)
    return chatbot(user_id, input_text)

iface = gr.Interface(
    fn=chat_interface,
    inputs=[
        gr.Textbox(lines=1, placeholder="Enter User ID", label="User ID"),
        gr.Textbox(lines=5, placeholder="Enter your question here", label="Enter your question here")
    ],
    outputs="text",
    title="Custom-trained AI Chatbot"
)

index = construct_index("Data")

iface.launch(share=True)
