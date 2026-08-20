#!/usr/bin/env python
# coding: utf-8

# ## nb_08_rag_embeddings
# 
# null

# In[1]:


import synapse.ml.spark.aifunc as aifunc

# Load prepared RAG knowledge
rag_knowledge_df = spark.table("rag_review_knowledge")

# Use only 5 rows for the first embedding test
embedding_test_df = (
    rag_knowledge_df
    .select(
        "review_id",
        "order_id",
        "document_text"
    )
    .limit(5)
)

# Generate embeddings
embedding_test_result_df = (
    embedding_test_df
    .ai.embed(
        input_col="document_text",
        output_col="embedding"
    )
)

display(embedding_test_result_df)


# In[2]:


embedding_test_result_df.select(
    "review_id",
    "document_text_embed_error"
).show(5, truncate=False)


# In[3]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip install -q sentence-transformers


# In[4]:


from sentence_transformers import SentenceTransformer

# Reload data because # The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip restarted the Python session
rag_knowledge_df = spark.table("rag_review_knowledge")

# Load multilingual embedding model
embedding_model = SentenceTransformer(
    "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
)

# Take only 5 documents
test_texts = [
    row["document_text"]
    for row in rag_knowledge_df.select("document_text").limit(5).collect()
]

# Generate embeddings
test_embeddings = embedding_model.encode(
    test_texts,
    normalize_embeddings=True
)

print("Model loaded successfully.")
print("Documents embedded:", len(test_embeddings))
print("Embedding dimensions:", test_embeddings.shape[1])
print("Shape:", test_embeddings.shape)


# In[ ]:




