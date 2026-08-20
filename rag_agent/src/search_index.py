import faiss
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer


# File paths
INDEX_PATH = "data/rag_faiss.index"
METADATA_PATH = "data/rag_metadata.parquet"

# Same embedding model used while building the FAISS index
MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"


# Load embedding model
print("Loading embedding model...")
model = SentenceTransformer(MODEL_NAME)


# Load FAISS index
print("Loading FAISS index...")
index = faiss.read_index(INDEX_PATH)


# Load metadata
print("Loading metadata...")
df = pd.read_parquet(METADATA_PATH)


# Validate index and metadata
print("FAISS vectors:", index.ntotal)
print("FAISS dimensions:", index.d)
print("Metadata rows:", len(df))

assert index.ntotal == len(df), \
    "FAISS vector count and metadata row count do not match!"


def search(query, top_k=5):

    # Convert user question into an embedding
    query_embedding = model.encode(
        [query],
        normalize_embeddings=True
    )

    # FAISS expects float32
    query_embedding = np.asarray(
        query_embedding,
        dtype="float32"
    )

    # Search FAISS
    scores, indices = index.search(
        query_embedding,
        top_k
    )

    results = []

    for score, idx in zip(scores[0], indices[0]):

        # Safety check
        if idx == -1:
            continue

        row = df.iloc[idx].to_dict()

        row["similarity_score"] = float(score)

        results.append(row)

    return results


# This part runs ONLY when this file is executed directly
if __name__ == "__main__":

    query = "customers complaining about late delivery"

    results = search(query, top_k=5)

    for i, result in enumerate(results, start=1):

        print("\n" + "=" * 80)
        print("RESULT:", i)
        print("Score:", result["similarity_score"])
        print("Text:", result["document_text"])