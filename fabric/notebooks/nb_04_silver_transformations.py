#!/usr/bin/env python
# coding: utf-8

# ## nb_04_silver_transformations
# 
# null

# In[1]:


orders_df = spark.table("bronze.orders")

orders_df.printSchema()
display(orders_df.limit(10))


# In[2]:


from pyspark.sql import functions as F

orders_clean_df = (
    orders_df
    .withColumn("order_id", F.trim(F.col("order_id")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn(
        "order_status",
        F.lower(F.trim(F.col("order_status")))
    )
    .withColumn(
        "order_purchase_timestamp",
        F.to_timestamp("order_purchase_timestamp")
    )
    .withColumn(
        "order_approved_at",
        F.to_timestamp("order_approved_at")
    )
    .withColumn(
        "order_delivered_carrier_date",
        F.to_timestamp("order_delivered_carrier_date")
    )
    .withColumn(
        "order_delivered_customer_date",
        F.to_timestamp("order_delivered_customer_date")
    )
    .withColumn(
        "order_estimated_delivery_date",
        F.to_timestamp("order_estimated_delivery_date")
    )
)

orders_clean_df.printSchema()
display(orders_clean_df.limit(10))


# In[3]:


orders_enriched_df = (
    orders_clean_df
    .withColumn(
        "order_purchase_date",
        F.to_date("order_purchase_timestamp")
    )
    .withColumn(
        "order_purchase_year",
        F.year("order_purchase_timestamp")
    )
    .withColumn(
        "order_purchase_month",
        F.date_format("order_purchase_timestamp", "yyyy-MM")
    )
    .withColumn(
        "is_delivered",
        F.col("order_status") == "delivered"
    )
    .withColumn(
        "is_cancelled",
        F.col("order_status") == "canceled"
    )
    .withColumn(
        "delivery_days",
        F.when(
            F.col("order_delivered_customer_date").isNotNull(),
            F.datediff(
                F.to_date("order_delivered_customer_date"),
                F.to_date("order_purchase_timestamp")
            )
        )
    )
    .withColumn(
        "delay_days",
        F.when(
            F.col("order_delivered_customer_date").isNotNull(),
            F.datediff(
                F.to_date("order_delivered_customer_date"),
                F.to_date("order_estimated_delivery_date")
            )
        )
    )
    .withColumn(
        "is_late",
        F.when(
            F.col("order_delivered_customer_date").isNotNull(),
            F.col("order_delivered_customer_date")
            > F.col("order_estimated_delivery_date")
        )
    )
)

display(
    orders_enriched_df.select(
        "order_id",
        "order_status",
        "order_purchase_date",
        "delivery_days",
        "delay_days",
        "is_late"
    ).limit(20)
)


# In[4]:


(
    orders_enriched_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.orders")
)

print("silver.orders created successfully")
print("Row count:", spark.table("silver.orders").count())


# In[5]:


order_items_df = spark.table("bronze.order_items")

order_items_df.printSchema()
display(order_items_df.limit(10))


# In[6]:


from pyspark.sql import functions as F

order_items_clean_df = (
    order_items_df
    .withColumn("order_id", F.trim(F.col("order_id")))
    .withColumn("product_id", F.trim(F.col("product_id")))
    .withColumn("seller_id", F.trim(F.col("seller_id")))
    .withColumn(
        "order_item_id",
        F.col("order_item_id").cast("int")
    )
    .withColumn(
        "shipping_limit_date",
        F.to_timestamp("shipping_limit_date")
    )
    .withColumn(
        "price",
        F.col("price").cast("double")
    )
    .withColumn(
        "freight_value",
        F.col("freight_value").cast("double")
    )
    .withColumn(
        "item_total_value",
        F.round(
            F.col("price") + F.col("freight_value"),
            2
        )
    )
)

order_items_clean_df.printSchema()

display(
    order_items_clean_df.select(
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
        "item_total_value"
    ).limit(20)
)


# In[7]:


(
    order_items_clean_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.order_items")
)

print("silver.order_items created successfully")
print("Row count:", spark.table("silver.order_items").count())


# In[8]:


order_reviews_df = spark.table("bronze.order_reviews")

order_reviews_df.printSchema()
display(order_reviews_df.limit(10))


# In[9]:


from pyspark.sql import functions as F

order_reviews_clean_df = (
    order_reviews_df
    .withColumn("review_id", F.trim(F.col("review_id")))
    .withColumn("order_id", F.trim(F.col("order_id")))
    .withColumn(
        "review_score",
        F.col("review_score").cast("int")
    )
    .withColumn(
        "review_comment_title",
        F.when(
            F.trim(F.col("review_comment_title")) == "",
            None
        ).otherwise(F.trim(F.col("review_comment_title")))
    )
    .withColumn(
        "review_comment_message",
        F.when(
            F.trim(F.col("review_comment_message")) == "",
            None
        ).otherwise(F.trim(F.col("review_comment_message")))
    )
    .withColumn(
        "review_creation_date",
        F.to_timestamp("review_creation_date")
    )
    .withColumn(
        "review_answer_timestamp",
        F.to_timestamp("review_answer_timestamp")
    )
)

order_reviews_clean_df.printSchema()

display(
    order_reviews_clean_df.select(
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp"
    ).limit(20)
)


# In[10]:


(
    order_reviews_clean_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.order_reviews")
)

print("silver.order_reviews created successfully")
print("Row count:", spark.table("silver.order_reviews").count())


# In[11]:


geolocation_df = spark.table("bronze.geolocation")

geolocation_df.printSchema()
display(geolocation_df.limit(10))


# In[12]:


from pyspark.sql import functions as F

geolocation_clean_df = (
    geolocation_df
    .withColumn(
        "geolocation_zip_code_prefix",
        F.trim(F.col("geolocation_zip_code_prefix"))
    )
    .withColumn(
        "geolocation_lat",
        F.col("geolocation_lat").cast("double")
    )
    .withColumn(
        "geolocation_lng",
        F.col("geolocation_lng").cast("double")
    )
    .withColumn(
        "geolocation_city",
        F.lower(F.trim(F.col("geolocation_city")))
    )
    .withColumn(
        "geolocation_state",
        F.upper(F.trim(F.col("geolocation_state")))
    )
    .dropDuplicates([
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_city",
        "geolocation_state"
    ])
)

print("Bronze rows:", geolocation_df.count())
print("Silver rows after exact duplicate removal:", geolocation_clean_df.count())

geolocation_clean_df.printSchema()
display(geolocation_clean_df.limit(20))


# In[13]:


(
    geolocation_clean_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.geolocation")
)

print("silver.geolocation created successfully")
print("Row count:", spark.table("silver.geolocation").count())


# In[14]:


silver_tables = [
    "customers",
    "sellers",
    "product_category_name_translation",
    "order_payments",
    "products",
    "orders",
    "order_items",
    "order_reviews",
    "geolocation"
]

validation_results = []

for table_name in silver_tables:
    bronze_count = spark.table(f"bronze.{table_name}").count()
    silver_count = spark.table(f"silver.{table_name}").count()

    validation_results.append(
        (
            table_name,
            bronze_count,
            silver_count,
            silver_count - bronze_count
        )
    )

validation_df = spark.createDataFrame(
    validation_results,
    [
        "table_name",
        "bronze_rows",
        "silver_rows",
        "row_difference"
    ]
)

display(validation_df.orderBy("table_name"))


# In[15]:


key_checks = [
    ("customers", ["customer_id"]),
    ("sellers", ["seller_id"]),
    ("products", ["product_id"]),
    ("orders", ["order_id"]),
    ("order_items", ["order_id", "order_item_id"]),
    ("order_payments", ["order_id", "payment_sequential"]),
    ("order_reviews", ["review_id", "order_id"]),
    (
        "product_category_name_translation",
        ["product_category_name"]
    ),
    (
        "geolocation",
        [
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng",
            "geolocation_city",
            "geolocation_state"
        ]
    )
]

key_results = []

for table_name, key_columns in key_checks:
    df = spark.table(f"silver.{table_name}")

    total_rows = df.count()
    distinct_keys = df.select(*key_columns).distinct().count()
    duplicate_rows = total_rows - distinct_keys

    key_results.append(
        (
            table_name,
            ", ".join(key_columns),
            total_rows,
            distinct_keys,
            duplicate_rows
        )
    )

key_validation_df = spark.createDataFrame(
    key_results,
    [
        "table_name",
        "key_columns",
        "total_rows",
        "distinct_keys",
        "duplicate_rows"
    ]
)

display(key_validation_df.orderBy("table_name"))


# In[16]:


relationship_checks = [
    (
        "orders.customer_id → customers.customer_id",
        spark.table("silver.orders")
        .select("customer_id")
        .join(
            spark.table("silver.customers").select("customer_id"),
            on="customer_id",
            how="left_anti"
        )
        .count()
    ),
    (
        "order_items.order_id → orders.order_id",
        spark.table("silver.order_items")
        .select("order_id")
        .join(
            spark.table("silver.orders").select("order_id"),
            on="order_id",
            how="left_anti"
        )
        .count()
    ),
    (
        "order_items.product_id → products.product_id",
        spark.table("silver.order_items")
        .select("product_id")
        .join(
            spark.table("silver.products").select("product_id"),
            on="product_id",
            how="left_anti"
        )
        .count()
    ),
    (
        "order_items.seller_id → sellers.seller_id",
        spark.table("silver.order_items")
        .select("seller_id")
        .join(
            spark.table("silver.sellers").select("seller_id"),
            on="seller_id",
            how="left_anti"
        )
        .count()
    ),
    (
        "order_payments.order_id → orders.order_id",
        spark.table("silver.order_payments")
        .select("order_id")
        .join(
            spark.table("silver.orders").select("order_id"),
            on="order_id",
            how="left_anti"
        )
        .count()
    ),
    (
        "order_reviews.order_id → orders.order_id",
        spark.table("silver.order_reviews")
        .select("order_id")
        .join(
            spark.table("silver.orders").select("order_id"),
            on="order_id",
            how="left_anti"
        )
        .count()
    )
]

relationship_validation_df = spark.createDataFrame(
    relationship_checks,
    ["relationship", "orphan_rows"]
)

display(relationship_validation_df)


# In[ ]:




