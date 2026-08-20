#!/usr/bin/env python
# coding: utf-8

# ## nb_01_bronze_ingestion
# 
# null

# In[2]:


seller_path = "Files/landing/olist/full_load/olist_sellers_dataset.csv"

sellers_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")   # Keep Bronze columns as source strings
    .option("delimiter", ",")
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", "true")
    .option("mode", "FAILFAST")       # Do not silently discard damaged rows
    .load(seller_path)
)

print("Rows loaded:", sellers_df.count())
print("Columns:", sellers_df.columns)

display(sellers_df.limit(10))


# In[11]:


# Write the complete sellers data as a Bronze Delta table
(
    sellers_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("bronze.sellers")
)

# Verify the saved table
seller_check = spark.sql("""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT seller_id) AS distinct_seller_ids,
        SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) AS null_seller_ids
    FROM bronze.sellers
""")

display(seller_check)


# In[13]:


# Remove the incomplete table created by the failed Copy Job
spark.sql("DROP TABLE IF EXISTS bronze.olist_sellers")

# Confirm the correct tables
spark.sql("SHOW TABLES IN bronze").show(truncate=False)


# In[4]:


# Remove the incomplete sellers table created by the failed Copy Job
spark.sql("DROP TABLE IF EXISTS bronze.olist_sellers")

print("Incomplete table bronze.olist_sellers removed.")


# In[5]:


reviews_path = "Files/landing/olist/full_load/olist_order_reviews_dataset.csv"

reviews_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")   # Preserve source values in Bronze
    .option("delimiter", ",")
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", "true")      # Review comments may span multiple lines
    .option("encoding", "UTF-8")
    .option("mode", "FAILFAST")       # Do not silently skip damaged records
    .load(reviews_path)
)

print("Rows loaded:", reviews_df.count())
print("Number of columns:", len(reviews_df.columns))
print("Columns:", reviews_df.columns)

display(reviews_df.limit(10))


# In[6]:


# Write the complete reviews dataset as a Bronze Delta table
(
    reviews_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("bronze.order_reviews")
)

# Verify the saved table
reviews_check = spark.sql("""
    SELECT
        COUNT(*) AS total_rows,
        SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_ids
    FROM bronze.order_reviews
""")

display(reviews_check)


# In[7]:


# Compare every source CSV row count with its Bronze table row count

source_to_bronze = {
    "olist_customers_dataset.csv": "bronze.customers",
    "olist_geolocation_dataset.csv": "bronze.geolocation",
    "olist_order_items_dataset.csv": "bronze.order_items",
    "olist_order_payments_dataset.csv": "bronze.order_payments",
    "olist_order_reviews_dataset.csv": "bronze.order_reviews",
    "olist_orders_dataset.csv": "bronze.orders",
    "olist_products_dataset.csv": "bronze.products",
    "olist_sellers_dataset.csv": "bronze.sellers",
    "product_category_name_translation.csv": "bronze.product_category_name_translation"
}

results = []

for file_name, table_name in source_to_bronze.items():
    file_path = f"Files/landing/olist/full_load/{file_name}"

    source_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("delimiter", ",")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .option("encoding", "UTF-8")
        .load(file_path)
    )

    source_rows = source_df.count()
    bronze_rows = spark.table(table_name).count()

    results.append(
        (
            file_name,
            table_name,
            source_rows,
            bronze_rows,
            bronze_rows - source_rows,
            "MATCH" if source_rows == bronze_rows else "MISMATCH"
        )
    )

validation_df = spark.createDataFrame(
    results,
    [
        "source_file",
        "bronze_table",
        "source_rows",
        "bronze_rows",
        "row_difference",
        "status"
    ]
)

display(validation_df.orderBy("source_file"))


# In[9]:


print("sellers exists:", spark.catalog.tableExists("bronze.sellers"))
print("order_reviews exists:", spark.catalog.tableExists("bronze.order_reviews"))


# In[12]:


spark.sql("SHOW TABLES IN bronze").show(truncate=False)


# In[ ]:




