import pandas as pd

DATA_PATH = "data/rag_review_knowledge.parquet"

df = pd.read_parquet(DATA_PATH)

print("Dataset loaded successfully.")
print("Rows:", len(df))
print("Columns:", len(df.columns))
print("\nColumn names:")
print(df.columns.tolist())

print("\nSample:")
print(df[["review_id", "order_id", "document_text"]].head())