import pandas as pd
import numpy as np
import faiss
from sentence_transformers import SentenceTransformer

DATA_PATH = "data/rag_review_knowledge.parquet"
INDEX_PATH = "data/rag_faiss.index"
METADATA_PATH = "data/rag_metadata.parquet"

MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"

# Load knowledge dataset
df = pd.read_parquet(DATA_PATH)

documents = df["document_text"].fillna("").tolist()

print("Documents to embed:", len(documents))

# Load cached embedding model
print("Loading embedding model...")
model = SentenceTransformer(MODEL_NAME)

# Create embeddings for all documents
embeddings = model.encode(
    documents,
    batch_size=64,
    show_progress_bar=True,
    normalize_embeddings=True
)

# FAISS works with float32 vectors
embeddings = np.asarray(embeddings, dtype="float32")

print("Embeddings created:", embeddings.shape)

# Create FAISS cosine-similarity index
dimension = embeddings.shape[1]

index = faiss.IndexFlatIP(dimension)
index.add(embeddings)

print("Vectors added to FAISS:", index.ntotal)

# Save index
faiss.write_index(index, INDEX_PATH)

# Save matching metadata in the exact same row order
df.reset_index(drop=True).to_parquet(
    METADATA_PATH,
    index=False
)

print("FAISS index saved:", INDEX_PATH)
print("Metadata saved:", METADATA_PATH)
print("RAG vector index build completed successfully.")