import sys
from pathlib import Path

import streamlit as st


# ---------------------------------------------------------
# Make src/ importable
# ---------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


from agent import ask_agent


# ---------------------------------------------------------
# Page configuration
# ---------------------------------------------------------
st.set_page_config(
    page_title="Olist Operations Assistant",
    page_icon="🛒",
    layout="wide",
)


# ---------------------------------------------------------
# Header
# ---------------------------------------------------------
st.title("🛒 Olist Operations Assistant")

st.caption(
    "Agentic AI assistant powered by Microsoft Fabric, "
    "Machine Learning and RAG."
)


# ---------------------------------------------------------
# Sidebar
# ---------------------------------------------------------
with st.sidebar:

    st.header("Capabilities")

    st.markdown(
        """
        **📊 Analytics**
        - Order status
        - Delivery performance
        - Sales performance
        - Sellers
        - Product categories
        - Review KPIs

        **🤖 Machine Learning**
        - Late-delivery predictions
        - High-risk orders
        - Risk probabilities

        **💬 RAG**
        - Customer complaints
        - Review insights
        - Delivery feedback

        **🧠 Agentic AI**
        - Automatic tool selection
        - Multi-tool reasoning
        """
    )

    st.divider()

    if st.button("Clear conversation"):
        st.session_state.messages = []
        st.rerun()


# ---------------------------------------------------------
# Chat history
# ---------------------------------------------------------
if "messages" not in st.session_state:
    st.session_state.messages = []


for message in st.session_state.messages:

    with st.chat_message(message["role"]):
        st.markdown(message["content"])


# ---------------------------------------------------------
# User input
# ---------------------------------------------------------
question = st.chat_input(
    "Ask about Olist sales, delivery, reviews or predictions..."
)


if question:

    # Save/show user message
    st.session_state.messages.append(
        {
            "role": "user",
            "content": question,
        }
    )

    with st.chat_message("user"):
        st.markdown(question)

    # Run Agent
    with st.chat_message("assistant"):

        with st.spinner("Analyzing your question..."):

            try:
                answer = ask_agent(question)

            except Exception as exc:
                answer = (
                    "The assistant encountered an unexpected error. "
                    "Please try again."
                )

        st.markdown(answer)

    # Save assistant response
    st.session_state.messages.append(
        {
            "role": "assistant",
            "content": answer,
        }
    )