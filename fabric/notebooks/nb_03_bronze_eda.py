#!/usr/bin/env python
# coding: utf-8

# ## nb_03_bronze_eda
# 
# null

# In[1]:


bronze_tables = [
    "customers",
    "geolocation",
    "order_items",
    "order_payments",
    "order_reviews",
    "orders",
    "products",
    "sellers",
    "product_category_name_translation"
]

schema_rows = []

for table_name in bronze_tables:
    df = spark.table(f"bronze.{table_name}")

    for position, field in enumerate(df.schema.fields, start=1):
        schema_rows.append((
            table_name,
            position,
            field.name,
            field.dataType.simpleString(),
            field.nullable
        ))

schema_summary_df = spark.createDataFrame(
    schema_rows,
    [
        "table_name",
        "column_position",
        "column_name",
        "data_type",
        "nullable"
    ]
)

display(
    schema_summary_df.orderBy(
        "table_name",
        "column_position"
    )
)


# In[3]:


from pyspark.sql import functions as F
from pyspark.sql.types import StringType

missing_rows = []

for table_name in bronze_tables:
    df = spark.table(f"bronze.{table_name}")
    total_rows = df.count()

    expressions = []

    for field in df.schema.fields:
        column = field.name

        expressions.append(
            F.sum(F.col(column).isNull().cast("int"))
            .alias(f"{column}__null")
        )

        if isinstance(field.dataType, StringType):
            expressions.append(
                F.sum(
                    (
                        F.col(column).isNotNull()
                        & (F.trim(F.col(column)) == "")
                    ).cast("int")
                ).alias(f"{column}__blank")
            )

    results = df.agg(*expressions).first().asDict()

    for field in df.schema.fields:
        column = field.name

        null_count = results.get(f"{column}__null", 0) or 0
        blank_count = results.get(f"{column}__blank", 0) or 0
        missing_count = null_count + blank_count

        missing_rows.append((
            table_name,
            column,
            total_rows,
            null_count,
            blank_count,
            missing_count,
            round((missing_count / total_rows) * 100, 2)
            if total_rows > 0 else 0.0
        ))

missing_profile_df = spark.createDataFrame(
    missing_rows,
    [
        "table_name",
        "column_name",
        "total_rows",
        "null_count",
        "blank_count",
        "missing_count",
        "missing_percentage"
    ]
)

display(
    missing_profile_df
    .filter(F.col("missing_count") > 0)
    .orderBy(
        F.desc("missing_percentage"),
        "table_name",
        "column_name"
    )
)


# In[4]:


from pyspark.sql import functions as F

key_checks = {
    "customers": ["customer_id"],
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "order_payments": ["order_id", "payment_sequential"],
    "order_reviews": ["review_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "product_category_name_translation": ["product_category_name"]
}

key_results = []

for table_name, key_columns in key_checks.items():
    df = spark.table(f"bronze.{table_name}")

    duplicate_df = (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
    )

    duplicate_summary = duplicate_df.agg(
        F.count("*").alias("duplicate_groups"),
        F.coalesce(
            F.sum(F.col("count") - 1),
            F.lit(0)
        ).alias("extra_duplicate_rows")
    ).first()

    key_results.append((
        table_name,
        " + ".join(key_columns),
        df.count(),
        df.dropDuplicates(key_columns).count(),
        duplicate_summary["duplicate_groups"],
        duplicate_summary["extra_duplicate_rows"]
    ))

key_check_df = spark.createDataFrame(
    key_results,
    [
        "table_name",
        "key_checked",
        "total_rows",
        "distinct_keys",
        "duplicate_groups",
        "extra_duplicate_rows"
    ]
)

display(key_check_df.orderBy("table_name"))


# In[5]:


from pyspark.sql import functions as F

reviews_df = spark.table("bronze.order_reviews")

duplicate_review_ids = (
    reviews_df
    .groupBy("review_id")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_reviews_detail = (
    reviews_df
    .join(
        duplicate_review_ids.select("review_id", "count"),
        on="review_id",
        how="inner"
    )
    .orderBy("review_id", "order_id")
)

display(duplicate_reviews_detail.limit(100))


# In[6]:


from pyspark.sql import functions as F

reviews_df = spark.table("bronze.order_reviews")

# Check duplicate composite keys
composite_duplicates = (
    reviews_df
    .groupBy("review_id", "order_id")
    .count()
    .filter(F.col("count") > 1)
)

# Check completely identical rows
exact_duplicates = (
    reviews_df
    .groupBy(*reviews_df.columns)
    .count()
    .filter(F.col("count") > 1)
)

composite_summary = composite_duplicates.agg(
    F.count("*").alias("duplicate_groups"),
    F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("extra_rows")
).first()

exact_summary = exact_duplicates.agg(
    F.count("*").alias("duplicate_groups"),
    F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("extra_rows")
).first()

results = [
    (
        "review_id + order_id",
        composite_summary["duplicate_groups"],
        composite_summary["extra_rows"]
    ),
    (
        "complete row",
        exact_summary["duplicate_groups"],
        exact_summary["extra_rows"]
    )
]

display(
    spark.createDataFrame(
        results,
        ["check_type", "duplicate_groups", "extra_duplicate_rows"]
    )
)


# In[7]:


from pyspark.sql import functions as F

relationships = [
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
    ("order_items", "seller_id", "sellers", "seller_id"),
    ("order_payments", "order_id", "orders", "order_id"),
    ("order_reviews", "order_id", "orders", "order_id"),
    (
        "products",
        "product_category_name",
        "product_category_name_translation",
        "product_category_name"
    )
]

relationship_results = []

for source_table, source_key, target_table, target_key in relationships:

    source_df = (
        spark.table(f"bronze.{source_table}")
        .select(F.col(source_key).alias("source_key"))
        .filter(F.col("source_key").isNotNull())
    )

    target_df = (
        spark.table(f"bronze.{target_table}")
        .select(F.col(target_key).alias("target_key"))
        .filter(F.col("target_key").isNotNull())
        .dropDuplicates(["target_key"])
    )

    orphan_df = source_df.join(
        target_df,
        source_df["source_key"] == target_df["target_key"],
        "left_anti"
    )

    relationship_results.append((
        source_table,
        source_key,
        target_table,
        target_key,
        source_df.count(),
        orphan_df.count(),
        orphan_df.select("source_key").distinct().count()
    ))

relationship_check_df = spark.createDataFrame(
    relationship_results,
    [
        "source_table",
        "source_column",
        "target_table",
        "target_column",
        "source_non_null_rows",
        "orphan_rows",
        "orphan_distinct_keys"
    ]
)

display(relationship_check_df)


# In[8]:


from pyspark.sql import functions as F

products_df = spark.table("bronze.products")
translation_df = spark.table(
    "bronze.product_category_name_translation"
)

missing_categories_df = (
    products_df
    .select("product_id", "product_category_name")
    .filter(F.col("product_category_name").isNotNull())
    .join(
        translation_df.select("product_category_name"),
        on="product_category_name",
        how="left_anti"
    )
)

display(
    missing_categories_df
    .groupBy("product_category_name")
    .agg(
        F.count("*").alias("product_count")
    )
    .orderBy(F.desc("product_count"))
)


# In[9]:


from pyspark.sql import functions as F

# Order status values
display(
    spark.table("bronze.orders")
    .groupBy("order_status")
    .agg(F.count("*").alias("order_count"))
    .orderBy(F.desc("order_count"))
)

# Review score values
display(
    spark.table("bronze.order_reviews")
    .groupBy("review_score")
    .agg(F.count("*").alias("review_count"))
    .orderBy("review_score")
)

# Payment type values
display(
    spark.table("bronze.order_payments")
    .groupBy("payment_type")
    .agg(F.count("*").alias("payment_count"))
    .orderBy(F.desc("payment_count"))
)


# In[10]:


from pyspark.sql import functions as F

orders_df = spark.table("bronze.orders")

date_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

date_results = []

for column_name in date_columns:
    result = orders_df.agg(
        F.sum(
            F.col(column_name).isNotNull().cast("int")
        ).alias("non_null_values"),

        F.sum(
            (
                F.col(column_name).isNotNull()
                & F.to_timestamp(F.col(column_name)).isNull()
            ).cast("int")
        ).alias("invalid_date_values")
    ).first()

    date_results.append(
        (
            column_name,
            result["non_null_values"],
            result["invalid_date_values"]
        )
    )

date_validation_df = spark.createDataFrame(
    date_results,
    [
        "column_name",
        "non_null_values",
        "invalid_date_values"
    ]
)

display(date_validation_df)


# In[11]:


from pyspark.sql import functions as F

orders_dates = (
    spark.table("bronze.orders")
    .withColumn(
        "purchase_ts",
        F.to_timestamp("order_purchase_timestamp")
    )
    .withColumn(
        "approved_ts",
        F.to_timestamp("order_approved_at")
    )
    .withColumn(
        "carrier_ts",
        F.to_timestamp("order_delivered_carrier_date")
    )
    .withColumn(
        "delivered_ts",
        F.to_timestamp("order_delivered_customer_date")
    )
    .withColumn(
        "estimated_ts",
        F.to_timestamp("order_estimated_delivery_date")
    )
)

timeline_check = orders_dates.agg(
    F.sum(
        (
            F.col("approved_ts").isNotNull()
            & (F.col("approved_ts") < F.col("purchase_ts"))
        ).cast("int")
    ).alias("approved_before_purchase"),

    F.sum(
        (
            F.col("carrier_ts").isNotNull()
            & F.col("approved_ts").isNotNull()
            & (F.col("carrier_ts") < F.col("approved_ts"))
        ).cast("int")
    ).alias("carrier_before_approval"),

    F.sum(
        (
            F.col("delivered_ts").isNotNull()
            & F.col("carrier_ts").isNotNull()
            & (F.col("delivered_ts") < F.col("carrier_ts"))
        ).cast("int")
    ).alias("delivered_before_carrier"),

    F.sum(
        (
            F.col("delivered_ts").isNotNull()
            & (F.col("delivered_ts") < F.col("purchase_ts"))
        ).cast("int")
    ).alias("delivered_before_purchase"),

    F.sum(
        (
            F.col("estimated_ts").isNotNull()
            & (F.col("estimated_ts") < F.col("purchase_ts"))
        ).cast("int")
    ).alias("estimated_before_purchase")
)

display(timeline_check)


# In[12]:


from pyspark.sql import functions as F

checks = []

def add_check(table_name, check_name, condition):
    df = spark.table(f"bronze.{table_name}")
    issue_count = df.filter(condition).count()
    checks.append((table_name, check_name, issue_count))


# Order items
order_items = spark.table("bronze.order_items")

add_check(
    "order_items",
    "negative_price",
    F.col("price").cast("double") < 0
)

add_check(
    "order_items",
    "negative_freight_value",
    F.col("freight_value").cast("double") < 0
)


# Payments
add_check(
    "order_payments",
    "negative_payment_value",
    F.col("payment_value").cast("double") < 0
)

add_check(
    "order_payments",
    "zero_or_negative_installments",
    F.col("payment_installments").cast("int") <= 0
)


# Reviews
add_check(
    "order_reviews",
    "review_score_outside_1_to_5",
    ~F.col("review_score").cast("int").between(1, 5)
)


# Products
product_numeric_columns = [
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm"
]

for column_name in product_numeric_columns:
    add_check(
        "products",
        f"{column_name}_zero_or_negative",
        F.col(column_name).isNotNull()
        & (F.col(column_name).cast("double") <= 0)
    )


# Geolocation
add_check(
    "geolocation",
    "latitude_outside_valid_range",
    ~F.col("geolocation_lat").cast("double").between(-90, 90)
)

add_check(
    "geolocation",
    "longitude_outside_valid_range",
    ~F.col("geolocation_lng").cast("double").between(-180, 180)
)


numeric_validation_df = spark.createDataFrame(
    checks,
    ["table_name", "check_name", "issue_count"]
)

display(
    numeric_validation_df.orderBy(
        F.desc("issue_count"),
        "table_name",
        "check_name"
    )
)


# In[13]:


from pyspark.sql import functions as F

# Products with invalid weight
invalid_product_weights = (
    spark.table("bronze.products")
    .filter(
        F.col("product_weight_g").isNotNull()
        & (F.col("product_weight_g").cast("double") <= 0)
    )
    .select(
        "product_id",
        "product_category_name",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    )
)

print("Products with zero or negative weight:")
display(invalid_product_weights)


# Payments with zero or negative installments
invalid_installments = (
    spark.table("bronze.order_payments")
    .filter(
        F.col("payment_installments").cast("int") <= 0
    )
    .select(
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value"
    )
)

print("Payments with zero or negative installments:")
display(invalid_installments)


# In[1]:


from pyspark.sql import functions as F

numeric_columns = {
    "order_items": ["price", "freight_value"],
    "order_payments": ["payment_value", "payment_installments"],
    "products": [
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    ]
}

profile_rows = []

for table_name, columns in numeric_columns.items():
    df = spark.table(f"bronze.{table_name}")

    for column_name in columns:
        values_df = (
            df.select(
                F.col(column_name).cast("double").alias("value")
            )
            .filter(F.col("value").isNotNull())
        )

        result = values_df.agg(
            F.count("*").alias("non_null_count"),
            F.min("value").alias("minimum"),
            F.expr(
                "percentile_approx(value, array(0.5, 0.95, 0.99), 10000)"
            ).alias("percentiles"),
            F.max("value").alias("maximum")
        ).first()

        percentiles = result["percentiles"]

        profile_rows.append((
            table_name,
            column_name,
            result["non_null_count"],
            result["minimum"],
            percentiles[0],
            percentiles[1],
            percentiles[2],
            result["maximum"]
        ))

numeric_profile_df = spark.createDataFrame(
    profile_rows,
    [
        "table_name",
        "column_name",
        "non_null_count",
        "minimum",
        "p50_median",
        "p95",
        "p99",
        "maximum"
    ]
)

display(
    numeric_profile_df.orderBy(
        "table_name",
        "column_name"
    )
)


# In[2]:


from pyspark.sql import functions as F

orders_delivery_df = (
    spark.table("bronze.orders")
    .withColumn(
        "purchase_ts",
        F.to_timestamp("order_purchase_timestamp")
    )
    .withColumn(
        "delivered_ts",
        F.to_timestamp("order_delivered_customer_date")
    )
    .withColumn(
        "estimated_ts",
        F.to_timestamp("order_estimated_delivery_date")
    )
    .filter(F.col("delivered_ts").isNotNull())
    .withColumn(
        "delivery_days",
        F.datediff(
            F.to_date("delivered_ts"),
            F.to_date("purchase_ts")
        )
    )
    .withColumn(
        "delay_days",
        F.datediff(
            F.to_date("delivered_ts"),
            F.to_date("estimated_ts")
        )
    )
    .withColumn(
        "is_late",
        F.when(F.col("delivered_ts") > F.col("estimated_ts"), 1)
         .otherwise(0)
    )
)

delivery_summary = orders_delivery_df.agg(
    F.count("*").alias("delivered_orders"),
    F.sum("is_late").alias("late_orders"),
    F.round(
        F.avg("is_late") * 100,
        2
    ).alias("late_delivery_percentage"),
    F.round(
        F.avg("delivery_days"),
        2
    ).alias("average_delivery_days"),
    F.round(
        F.avg(
            F.when(
                F.col("is_late") == 1,
                F.col("delay_days")
            )
        ),
        2
    ).alias("average_delay_days_for_late_orders"),
    F.min("delivery_days").alias("minimum_delivery_days"),
    F.max("delivery_days").alias("maximum_delivery_days")
)

display(delivery_summary)


# In[3]:


from pyspark.sql import functions as F

monthly_delivery = (
    orders_delivery_df
    .withColumn(
        "purchase_month",
        F.date_format("purchase_ts", "yyyy-MM")
    )
    .groupBy("purchase_month")
    .agg(
        F.count("*").alias("delivered_orders"),
        F.sum("is_late").alias("late_orders"),
        F.round(
            F.avg("is_late") * 100,
            2
        ).alias("late_delivery_percentage"),
        F.round(
            F.avg("delivery_days"),
            2
        ).alias("average_delivery_days")
    )
    .orderBy("purchase_month")
)

display(monthly_delivery)


# # Bronze EDA Findings
# 
# ## Data completeness
# - All 9 source files match their Bronze table row counts.
# - Missing review titles and messages are expected because comments are optional.
# - Missing delivery timestamps mainly relate to cancelled, unavailable, or unfinished orders.
# - Some product attributes contain missing values.
# 
# ## Keys and duplicates
# - Main business keys are unique.
# - `order_reviews` uses `review_id + order_id` as the reliable composite key.
# - Geolocation contains natural source duplicates that will be handled in Silver.
# 
# ## Relationships
# - All major foreign-key relationships are valid.
# - 13 products across 2 categories do not have English category translations.
# 
# ## Data-quality issues
# - 4 products have a weight of zero.
# - 2 credit-card payments have zero installments.
# - 3 payments use the value `not_defined`.
# - All order timestamps can be converted successfully.
# - Major order timeline relationships are valid.
# 
# ## Delivery performance
# - Delivered orders: 96,476
# - Late orders: 7,827
# - Late-delivery rate: 8.11%
# - Average delivery time: 12.5 days
# - Average delay for late orders: 8.87 days
# - Late-delivery performance changes noticeably over time.
# 

# In[1]:


translation_df = spark.table("bronze.product_category_name_translation")

print(translation_df.columns)
display(translation_df.limit(10))


# In[3]:


source_df = spark.table("bronze.product_category_name_translation")

# Only 71 rows, so materialize them before overwriting the same table
clean_rows = [
    (
        row["product_category_name"],
        row["product_category_name_english\r"]
    )
    for row in source_df.collect()
]

clean_df = spark.createDataFrame(
    clean_rows,
    [
        "product_category_name",
        "product_category_name_english"
    ]
)

(
    clean_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("bronze.product_category_name_translation")
)

print(clean_df.columns)
display(clean_df.limit(10))


# In[ ]:




