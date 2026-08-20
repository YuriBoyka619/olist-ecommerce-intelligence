#!/usr/bin/env python
# coding: utf-8

# ## nb_01_source_data_profiling
# 
# null

# # Olist Source Data Profiling
# 
# **Notebook:** `nb_01_source_data_profiling`  
# **Phase:** Phase 1 — Data Foundation  
# **Purpose:** Validate and profile the nine original Olist CSV source files before Bronze ingestion.  
# **Source location:** `Files/landing/olist/full_load`  
# **Lakehouse:** `lh_olist_ecommerce_dev`

# In[2]:


# ---------------------------------------------------------
# 1. Validate the Olist source folder
# ---------------------------------------------------------

SOURCE_PATH = "Files/landing/olist/full_load"

source_files = notebookutils.fs.ls(SOURCE_PATH)

print(f"Source path: {SOURCE_PATH}")
print(f"Number of item found: {len(source_files)}")

for file in source_files:
    print(f"{file.name} | {file.size} bytes)")


# In[3]:


# ---------------------------------------------------------
# 2. Validate the expected Olist source files
# ---------------------------------------------------------

EXPECTED_FILES = {
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv"
}

# Keep only CSV files found in the source folder
actual_csv_items = [
    item for item in source_files
    if item.name.lower().endswith(".csv")
]

actual_files = [item.name for item in actual_csv_items]

# Validation checks
missing_files = sorted(EXPECTED_FILES - set(actual_files))
unexpected_files = sorted(set(actual_files) - EXPECTED_FILES)

duplicate_files = sorted({
    file_name
    for file_name in actual_files
    if actual_files.count(file_name) > 1
})

empty_files = sorted([
    item.name
    for item in actual_csv_items
    if item.size == 0
])

print(f"Expected CSV files : {len(EXPECTED_FILES)}")
print(f"Actual CSV files   : {len(actual_files)}")
print(f"Missing files      : {missing_files}")
print(f"Unexpected files   : {unexpected_files}")
print(f"Duplicate files    : {duplicate_files}")
print(f"Empty files        : {empty_files}")

# Stop execution if the source set is not valid
if missing_files or unexpected_files or duplicate_files or empty_files:
    raise ValueError(
        "Source-file validation failed. "
        "Review the missing, unexpected, duplicate, or empty files."
    )

if len(actual_files) != len(EXPECTED_FILES):
    raise ValueError(
        f"Expected {len(EXPECTED_FILES)} CSV files, "
        f"but found {len(actual_files)}."
    )

print("\nSource-file validation passed successfully.")


# In[4]:


# ---------------------------------------------------------
# 3. Create the Olist dataset inventory
# ---------------------------------------------------------

from pyspark.sql import Row
from pyspark.sql import functions as F

# Business metadata cannot be understood reliably by Spark,
# so we define it explicitly after studying the source files.
SOURCE_METADATA = {
    "olist_customers_dataset.csv": {
        "source_id": "SRC-001",
        "logical_entity": "Customers",
        "business_description": (
            "Contains customer identifiers and customer geographic information."
        ),
        "data_grain": "One row per customer_id",
        "candidate_primary_key": "customer_id",
        "main_date_column": None
    },

    "olist_geolocation_dataset.csv": {
        "source_id": "SRC-002",
        "logical_entity": "Geolocation",
        "business_description": (
            "Contains Brazilian ZIP-code prefixes with latitude, longitude, city and state."
        ),
        "data_grain": (
            "One row per geolocation observation; ZIP-code prefixes may repeat"
        ),
        "candidate_primary_key": None,
        "main_date_column": None
    },

    "olist_order_items_dataset.csv": {
        "source_id": "SRC-003",
        "logical_entity": "Order Items",
        "business_description": (
            "Contains individual products and sellers associated with each order."
        ),
        "data_grain": "One row per order_id and order_item_id",
        "candidate_primary_key": "order_id + order_item_id",
        "main_date_column": "shipping_limit_date"
    },

    "olist_order_payments_dataset.csv": {
        "source_id": "SRC-004",
        "logical_entity": "Order Payments",
        "business_description": (
            "Contains payment methods, instalments and payment values for orders."
        ),
        "data_grain": "One row per order_id and payment_sequential",
        "candidate_primary_key": "order_id + payment_sequential",
        "main_date_column": None
    },

    "olist_order_reviews_dataset.csv": {
        "source_id": "SRC-005",
        "logical_entity": "Order Reviews",
        "business_description": (
            "Contains customer review scores, comments and review timestamps."
        ),
        "data_grain": "One row per submitted review",
        "candidate_primary_key": "review_id + order_id",
        "main_date_column": "review_creation_date"
    },

    "olist_orders_dataset.csv": {
        "source_id": "SRC-006",
        "logical_entity": "Orders",
        "business_description": (
            "Contains the order lifecycle, status and delivery timestamps."
        ),
        "data_grain": "One row per order_id",
        "candidate_primary_key": "order_id",
        "main_date_column": "order_purchase_timestamp"
    },

    "olist_products_dataset.csv": {
        "source_id": "SRC-007",
        "logical_entity": "Products",
        "business_description": (
            "Contains product category, dimensions, weight and descriptive attributes."
        ),
        "data_grain": "One row per product_id",
        "candidate_primary_key": "product_id",
        "main_date_column": None
    },

    "olist_sellers_dataset.csv": {
        "source_id": "SRC-008",
        "logical_entity": "Sellers",
        "business_description": (
            "Contains seller identifiers and geographic information."
        ),
        "data_grain": "One row per seller_id",
        "candidate_primary_key": "seller_id",
        "main_date_column": None
    },

    "product_category_name_translation.csv": {
        "source_id": "SRC-009",
        "logical_entity": "Product Category Translation",
        "business_description": (
            "Maps Portuguese product-category names to English names."
        ),
        "data_grain": "One row per Portuguese product category",
        "candidate_primary_key": "product_category_name",
        "main_date_column": None
    }
}

# Create a lookup containing the uploaded file sizes.
file_size_lookup = {
    item.name: item.size
    for item in actual_csv_items
}

inventory_records = []

for file_name in sorted(EXPECTED_FILES):

    file_path = f"{SOURCE_PATH}/{file_name}"
    metadata = SOURCE_METADATA[file_name]

    print(f"Profiling: {file_name}")

    # Read the CSV with the first row treated as column names.
    source_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "PERMISSIVE")
        .csv(file_path)
    )

    row_count = source_df.count()
    column_count = len(source_df.columns)
    file_size_bytes = file_size_lookup[file_name]

    inventory_records.append(
        Row(
            source_id=metadata["source_id"],
            source_file_name=file_name,
            logical_entity=metadata["logical_entity"],
            business_description=metadata["business_description"],
            file_format="CSV",
            relative_source_path=file_path,
            file_size_bytes=file_size_bytes,
            file_size_mb=round(file_size_bytes / (1024 * 1024), 2),
            row_count=row_count,
            column_count=column_count,
            data_grain=metadata["data_grain"],
            candidate_primary_key=metadata["candidate_primary_key"],
            main_date_column=metadata["main_date_column"],
            load_type="Initial Full Load",
            data_sensitivity="Public / Anonymized",
            assessment_status="Inventory Completed"
        )
    )

dataset_inventory_df = (
    spark.createDataFrame(inventory_records)
    .orderBy("source_id")
)

print("\nDataset inventory created successfully.")

display(dataset_inventory_df)


# In[ ]:




