import pandas as pd
from sentence_transformers import SentenceTransformer

DATA_PATH = "data/rag_review_knowledge.parquet"
MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"

# Load RAG knowledge
df = pd.read_parquet(DATA_PATH)

# Load multilingual embedding model
print("Loading embedding model...")
model = SentenceTransformer(MODEL_NAME)

# Test only 5 documents first
test_documents = df["document_text"].head(5).tolist()

embeddings = model.encode(
    test_documents,
    normalize_embeddings=True
)

print("Embedding test successful.")
print("Documents:", len(test_documents))
print("Embedding dimensions:", embeddings.shape[1])
print("Shape:", embeddings.shape)