from google import genai
from dotenv import load_dotenv
from build_context import build_context

load_dotenv()

MODEL_NAME = "gemini-3.6-flash"

client = genai.Client()

def answer_question(question, top_k=5):
    # Retrieve relevant Olist reviews from FAISS
    context = build_context(question, top_k=top_k)

    prompt = f"""
You are an AI assistant analyzing the Olist e-commerce dataset.

Answer the user's question using ONLY the retrieved customer-review context below.

Rules:
- Base the answer only on the provided context.
- Do not invent facts.
- If the context is insufficient, clearly say so.
- Customer reviews may be in Portuguese, but answer in English.
- Summarize the main patterns clearly and concisely.

USER QUESTION:
{question}

RETRIEVED CONTEXT:
{context}

ANSWER:
"""

    response = client.models.generate_content(
        model=MODEL_NAME,
        contents=prompt
    )

    return response.text


if __name__ == "__main__":
    question = input("Ask a question about Olist customer reviews: ")

    answer = answer_question(question)

    print("\n" + "=" * 80)
    print("QUESTION:")
    print(question)

    print("\nRAG ANSWER:")
    print(answer)