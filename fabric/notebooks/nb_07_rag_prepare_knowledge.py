#!/usr/bin/env python
# coding: utf-8

# ## nb_07_rag_prepare_knowledge
# 
# null

# In[1]:


from pyspark.sql import functions as F

reviews_df = spark.table("silver.order_reviews")

print("Review rows:", reviews_df.count())
print("\nColumns:")
print(reviews_df.columns)

reviews_df.select(
    "review_id",
    "order_id",
    "review_score",
    "review_comment_title",
    "review_comment_message"
).show(10, truncate=False)


# In[2]:


# --------------------------------------------------
# Build clean textual review dataset for RAG
# --------------------------------------------------

rag_reviews_df = (
    reviews_df
    .withColumn(
        "review_title_clean",
        F.trim(
            F.coalesce(
                F.col("review_comment_title"),
                F.lit("")
            )
        )
    )
    .withColumn(
        "review_message_clean",
        F.trim(
            F.coalesce(
                F.col("review_comment_message"),
                F.lit("")
            )
        )
    )
    .withColumn(
        "review_text",
        F.trim(
            F.concat_ws(
                " ",
                F.col("review_title_clean"),
                F.col("review_message_clean")
            )
        )
    )
    .filter(F.length("review_text") > 0)
)

print("Total reviews:", reviews_df.count())
print("Reviews with usable text:", rag_reviews_df.count())

rag_reviews_df.select(
    "review_id",
    "order_id",
    "review_score",
    "review_text"
).show(10, truncate=False)


# In[3]:


# --------------------------------------------------
# Enrich RAG reviews with order information
# --------------------------------------------------

orders_df = spark.table("silver.orders")

rag_enriched_df = (
    rag_reviews_df
    .join(
        orders_df.select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_delivered_customer_date",
            "order_estimated_delivery_date"
        ),
        on="order_id",
        how="left"
    )
    .withColumn(
        "is_late",
        (
            F.col("order_delivered_customer_date")
            > F.col("order_estimated_delivery_date")
        ).cast("int")
    )
)

print("RAG rows after order enrichment:", rag_enriched_df.count())

rag_enriched_df.select(
    "order_id",
    "review_score",
    "order_status",
    "is_late",
    "review_text"
).show(10, truncate=False)


# In[4]:


# --------------------------------------------------
# Inspect product-related Silver tables
# --------------------------------------------------

order_items_df = spark.table("silver.order_items")
products_df = spark.table("silver.products")
category_translation_df = spark.table("silver.product_category_name_translation")

print("ORDER ITEMS COLUMNS:")
print(order_items_df.columns)

print("\nPRODUCTS COLUMNS:")
print(products_df.columns)

print("\nCATEGORY TRANSLATION COLUMNS:")
print(category_translation_df.columns)

print("\nSample products:")
products_df.show(5, truncate=False)

print("\nSample category translations:")
category_translation_df.show(5, truncate=False)


# In[5]:


# --------------------------------------------------
# Build order-level product context
# --------------------------------------------------

order_product_context_df = (
    order_items_df
    .select(
        "order_id",
        "product_id"
    )
    .join(
        products_df.select(
            "product_id",
            "product_category_name_display"
        ),
        on="product_id",
        how="left"
    )
    .groupBy("order_id")
    .agg(
        F.countDistinct("product_id").alias("product_count"),
        F.collect_set(
            F.coalesce(
                F.col("product_category_name_display"),
                F.lit("Unknown")
            )
        ).alias("product_categories")
    )
    .withColumn(
        "product_categories_text",
        F.concat_ws(
            ", ",
            F.sort_array("product_categories")
        )
    )
)

# Join product context to RAG reviews
rag_enriched_df = (
    rag_enriched_df
    .join(
        order_product_context_df,
        on="order_id",
        how="left"
    )
)

print("RAG rows after product enrichment:", rag_enriched_df.count())

rag_enriched_df.select(
    "order_id",
    "review_score",
    "is_late",
    "product_count",
    "product_categories_text",
    "review_text"
).show(10, truncate=False)


# In[6]:


# --------------------------------------------------
# Build clean RAG documents
# --------------------------------------------------

rag_documents_df = (
    rag_enriched_df

    # Clean formatting characters
    .withColumn(
        "product_categories_clean",
        F.regexp_replace(
            F.coalesce(
                F.col("product_categories_text"),
                F.lit("Unknown")
            ),
            r"[\r\n\t]+",
            " "
        )
    )

    .withColumn(
        "review_text_clean",
        F.regexp_replace(
            F.col("review_text"),
            r"[\r\n\t]+",
            " "
        )
    )

    # Human-readable delivery status
    .withColumn(
        "delivery_status",
        F.when(F.col("is_late") == 1, "Late")
         .when(F.col("is_late") == 0, "On time")
         .otherwise("Unknown")
    )

    # Text that will later be embedded
    .withColumn(
        "document_text",
        F.concat_ws(
            "\n",
            F.concat(
                F.lit("Review score: "),
                F.col("review_score").cast("string")
            ),
            F.concat(
                F.lit("Order status: "),
                F.col("order_status")
            ),
            F.concat(
                F.lit("Delivery status: "),
                F.col("delivery_status")
            ),
            F.concat(
                F.lit("Product categories: "),
                F.col("product_categories_clean")
            ),
            F.concat(
                F.lit("Customer review: "),
                F.col("review_text_clean")
            )
        )
    )
)

print("RAG documents:", rag_documents_df.count())

rag_documents_df.select(
    "review_id",
    "order_id",
    "document_text"
).show(5, truncate=False)


# In[7]:


# --------------------------------------------------
# Save prepared RAG knowledge dataset
# --------------------------------------------------

rag_knowledge_df = (
    rag_documents_df
    .select(
        "review_id",
        "order_id",
        "review_score",
        "order_status",
        "is_late",
        "delivery_status",
        "product_count",
        "product_categories_clean",
        "review_text_clean",
        "document_text"
    )
)

(
    rag_knowledge_df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("rag_review_knowledge")
)

checkpoint_df = spark.table("rag_review_knowledge")

print("RAG knowledge dataset saved successfully.")
print("Rows:", checkpoint_df.count())
print("Columns:", len(checkpoint_df.columns))


# In[1]:


# Export RAG knowledge as one Parquet file

rag_export_df = spark.table("rag_review_knowledge")

rag_export_pd = rag_export_df.toPandas()

rag_export_pd.to_parquet(
    "/lakehouse/default/Files/rag_review_knowledge.parquet",
    index=False
)

print("RAG dataset exported successfully.")
print("Rows:", len(rag_export_pd))


# In[2]:


import os

path = "/lakehouse/default/Files/rag_review_knowledge.parquet"

print("Exists:", os.path.exists(path))
print("Default Files contents:")
print(os.listdir("/lakehouse/default/Files"))


# In[ ]:




