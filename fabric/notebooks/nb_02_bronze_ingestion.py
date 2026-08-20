#!/usr/bin/env python
# coding: utf-8

# ## nb_02_bronze_ingestion
# 
# New notebook

# # Bronze Ingestion
# 
# **Notebook:** `nb_02_bronze_ingestion`  
# **Purpose:** Load original Olist CSV files from the landing folder into Bronze Delta tables.  
# **Source:** `Files/landing/olist/full_load`  
# **Target schema:** `bronze`

# In[2]:


# ---------------------------------------------------------
# 1. Load the Orders source file into the Bronze layer
# ---------------------------------------------------------

from datetime import datetime, timezone
from pyspark.sql import functions as F

SOURCE_FILE = "olist_orders_dataset.csv"
SOURCE_PATH = f"Files/landing/olist/full_load/{SOURCE_FILE}"
TARGET_TABLE = "bronze.orders"

# Unique identifier for this ingestion run
batch_id = datetime.now(timezone.utc).strftime(
    "full_%Y%m%dT%H%M%SZ"
)

# Read the original CSV file
orders_source_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .option("mode", "FAILFAST")
    .csv(SOURCE_PATH)
)

# Add ingestion metadata
bronze_orders_df = (
    orders_source_df
    .withColumn(
        "_ingestion_timestamp",
        F.current_timestamp()
    )
    .withColumn(
        "_source_file_name",
        F.lit(SOURCE_FILE)
    )
    .withColumn(
        "_batch_id",
        F.lit(batch_id)
    )
)

# Save as a Delta table in the Bronze schema
(
    bronze_orders_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

print(f"Successfully loaded: {SOURCE_FILE}")
print(f"Target table: {TARGET_TABLE}")
print(f"Batch ID: {batch_id}")
print(f"Rows loaded: {bronze_orders_df.count():,}")


# In[ ]:




