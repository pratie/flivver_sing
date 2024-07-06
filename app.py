import os
import sys
import logging
import sqlite3
import nest_asyncio
import streamlit as st
from dotenv import load_dotenv
from llama_index.core import GPTVectorStoreIndex, SimpleDirectoryReader, ServiceContext, StorageContext, load_index_from_storage
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.core import Settings, set_global_service_context
from llama_index.core.query_engine.router_query_engine import RouterQueryEngine
from llama_index.core.selectors.llm_selectors import LLMSingleSelector
from llama_index.core.tools.query_engine import QueryEngineTool
from llama_index.llms.openai import OpenAI
from llama_index.embeddings.openai import OpenAIEmbedding
from langchain.schema import SystemMessage, HumanMessage, AIMessage

# Load environment variables from .env file
load_dotenv()

# Initialize async environment
nest_asyncio.apply()

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.handlers = []
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
logger.addHandler(handler)

#os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")

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

# Define helper functions for chat history
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

# Initialize Streamlit page
def init_page():
    st.set_page_config(page_title='Personal Chatbot', page_icon='🤖')
    st.header('Knowledge Query Assistant')
    st.write("I'm here to help you get information from your file.")
    st.sidebar.title('Options')

def select_llm():
    return OpenAI(temperature=0.5, model_name="gpt-3.5-turbo")

def select_embedding():
    return OpenAIEmbedding()

def init_messages():
    clear_button = st.sidebar.button('Clear Conversation', key='clear')
    if clear_button or 'messages' not in st.session_state:
        st.session_state.messages = [
            SystemMessage(
                content='You are a helpful AI assistant. Reply your answer in markdown format.'
            )
        ]

def get_answer(query_engine, query):
    response = query_engine.query(query)
    return response.response, response.metadata

# Main function
def main():
    init_page()
  
    user_id = st.sidebar.text_input('Enter User ID:', key='user_id')
    if not user_id:
        st.sidebar.warning("Please enter your User ID.")
        return

    documents = SimpleDirectoryReader('./Data').load_data()
    
    # Load llm & embed model
    llm = select_llm()
    embed = select_embedding()
    service_context = ServiceContext.from_defaults(llm=llm, embed_model=embed)

    # Setup query engine
    index = GPTVectorStoreIndex.from_documents(documents, service_context=service_context)
    query_engine = index.as_query_engine()

    init_messages()

    # Display chat history
    history = get_chat_history(user_id)
    for msg, resp in history:
        st.session_state.messages.append(HumanMessage(content=msg))
        st.session_state.messages.append(AIMessage(content=resp))

    # Get user input -> Generate the answer
    if user_input := st.chat_input('Input your question!'):
        st.session_state.messages.append(HumanMessage(content=user_input))
        with st.spinner('Bot is typing ...'):
            answer, meta_data = get_answer(query_engine, user_input)
        save_chat_history(user_id, user_input, answer)
        st.session_state.messages.append(AIMessage(content=answer))
                
    # Show all the messages of the conversation
    messages = st.session_state.get('messages', [])
    for message in messages:
        if isinstance(message, AIMessage):
            with st.chat_message('assistant'):
                st.markdown(message.content)
        elif isinstance(message, HumanMessage):
            with st.chat_message('user'):
                st.markdown(message.content)

if not os.listdir('./Data'):
    st.write('No file is saved yet.')

if __name__ == '__main__':
    main()
