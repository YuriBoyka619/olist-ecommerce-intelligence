#!/usr/bin/env python
# coding: utf-8

# ## nb_05a_ml_feature_refresh
# 
# null

# In[1]:


from pyspark.sql import functions as F

# Load latest Silver tables
orders_df = spark.table("silver.orders")
order_items_df = spark.table("silver.order_items")
customers_df = spark.table("silver.customers")
products_df = spark.table("silver.products")
sellers_df = spark.table("silver.sellers")
geolocation_df = spark.table("silver.geolocation")
payments_df = spark.table("silver.order_payments")

print("Silver source tables loaded successfully.")

print("Orders:", orders_df.count())
print("Order Items:", order_items_df.count())
print("Customers:", customers_df.count())
print("Products:", products_df.count())
print("Sellers:", sellers_df.count())
print("Geolocation:", geolocation_df.count())
print("Payments:", payments_df.count())


# In[2]:


# --------------------------------------------------
# Create eligible order-level ML base
# --------------------------------------------------

ml_base_df = (
    orders_df

    # Only completed deliveries where actual delivery date exists
    .filter(
        (F.col("order_status") == "delivered") &
        F.col("order_delivered_customer_date").isNotNull() &
        F.col("order_estimated_delivery_date").isNotNull()
    )

    # Target: 1 = delivered later than estimated date
    .withColumn(
        "label",
        (
            F.col("order_delivered_customer_date") >
            F.col("order_estimated_delivery_date")
        ).cast("int")
    )

    .select(
        "order_id",
        "customer_id",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_estimated_delivery_date",
        "label"
    )
)

print("Eligible ML orders:", ml_base_df.count())

ml_base_df.groupBy("label").count().orderBy("label").show()


# In[3]:


# --------------------------------------------------
# Add customer geography
# --------------------------------------------------

customer_features_df = customers_df.select(
    "customer_id",
    "customer_state",
    "customer_zip_code_prefix"
)

ml_base_df = (
    ml_base_df
    .join(
        customer_features_df,
        on="customer_id",
        how="left"
    )
)

print("ML rows after customer join:", ml_base_df.count())

ml_base_df.select(
    F.sum(F.col("customer_state").isNull().cast("int"))
        .alias("missing_customer_state"),
    F.sum(F.col("customer_zip_code_prefix").isNull().cast("int"))
        .alias("missing_customer_zip")
).show()


# In[4]:


# --------------------------------------------------
# Build order-item + product features
# --------------------------------------------------

products_features_df = (
    products_df
    .withColumn(
        "product_volume_cm3",
        F.col("product_length_cm") *
        F.col("product_height_cm") *
        F.col("product_width_cm")
    )
    .select(
        "product_id",
        "product_category_name",
        "product_weight_g",
        "product_volume_cm3",
        "product_photos_qty"
    )
)

item_product_df = (
    order_items_df
    .join(
        products_features_df,
        on="product_id",
        how="left"
    )
)

order_item_product_features_df = (
    item_product_df
    .groupBy("order_id")
    .agg(
        F.count("*").alias("item_count"),
        F.countDistinct("seller_id").alias("seller_count"),
        F.countDistinct("product_id").alias("product_count"),

        F.sum("price").alias("total_price"),
        F.sum("freight_value").alias("total_freight"),

        F.sum(
            F.col("price") + F.col("freight_value")
        ).alias("total_item_value"),

        F.avg("price").alias("avg_item_price"),

        F.countDistinct(
            "product_category_name"
        ).alias("category_count"),

        F.avg("product_weight_g")
            .alias("avg_product_weight_g"),

        F.max("product_weight_g")
            .alias("max_product_weight_g"),

        F.avg("product_volume_cm3")
            .alias("avg_product_volume_cm3"),

        F.max("product_volume_cm3")
            .alias("max_product_volume_cm3"),

        F.avg("product_photos_qty")
            .alias("avg_product_photos_qty")
    )
)

# Join features to ML base
ml_base_df = (
    ml_base_df
    .join(
        order_item_product_features_df,
        on="order_id",
        how="left"
    )
)

print("ML rows after item/product join:", ml_base_df.count())

ml_base_df.select(
    F.sum(F.col("item_count").isNull().cast("int"))
        .alias("missing_item_features"),
    F.sum(F.col("avg_product_weight_g").isNull().cast("int"))
        .alias("missing_product_weight"),
    F.sum(F.col("avg_product_photos_qty").isNull().cast("int"))
        .alias("missing_product_photos")
).show()


# In[5]:


# --------------------------------------------------
# Build seller features per order
# --------------------------------------------------

seller_features_df = sellers_df.select(
    "seller_id",
    "seller_state",
    "seller_zip_code_prefix"
)

item_seller_df = (
    order_items_df
    .join(
        seller_features_df,
        on="seller_id",
        how="left"
    )
)

order_seller_features_df = (
    item_seller_df
    .groupBy("order_id")
    .agg(
        F.countDistinct("seller_state")
            .alias("seller_state_count"),

        F.countDistinct("seller_zip_code_prefix")
            .alias("seller_zip_count"),

        F.collect_set("seller_state")
            .alias("seller_states")
    )
)

# Join seller features to ML base
ml_base_df = (
    ml_base_df
    .join(
        order_seller_features_df,
        on="order_id",
        how="left"
    )
    .withColumn(
        "seller_customer_same_state",
        F.expr(
            "array_contains(seller_states, customer_state)"
        ).cast("int")
    )
    .drop("seller_states")
)

print("ML rows after seller join:", ml_base_df.count())

ml_base_df.select(
    F.sum(
        F.col("seller_state_count").isNull().cast("int")
    ).alias("missing_seller_features")
).show()


# In[6]:


# --------------------------------------------------
# Create one representative lat/lng per ZIP prefix
# --------------------------------------------------

zip_geo_df = (
    geolocation_df
    .groupBy("geolocation_zip_code_prefix")
    .agg(
        F.avg("geolocation_lat").alias("lat"),
        F.avg("geolocation_lng").alias("lng")
    )
)

# Customer coordinates
customer_geo_df = (
    ml_base_df
    .select(
        "order_id",
        "customer_zip_code_prefix"
    )
    .join(
        zip_geo_df
        .withColumnRenamed("geolocation_zip_code_prefix", "customer_zip_code_prefix")
        .withColumnRenamed("lat", "customer_lat")
        .withColumnRenamed("lng", "customer_lng"),
        on="customer_zip_code_prefix",
        how="left"
    )
)

# Seller coordinates for every order item
seller_geo_df = (
    order_items_df
    .select(
        "order_id",
        "seller_id"
    )
    .join(
        sellers_df.select(
            "seller_id",
            "seller_zip_code_prefix"
        ),
        on="seller_id",
        how="left"
    )
    .join(
        zip_geo_df
        .withColumnRenamed("geolocation_zip_code_prefix", "seller_zip_code_prefix")
        .withColumnRenamed("lat", "seller_lat")
        .withColumnRenamed("lng", "seller_lng"),
        on="seller_zip_code_prefix",
        how="left"
    )
)

# --------------------------------------------------
# Join customer + seller coordinates
# --------------------------------------------------

distance_base_df = (
    seller_geo_df
    .join(
        customer_geo_df.select(
            "order_id",
            "customer_lat",
            "customer_lng"
        ),
        on="order_id",
        how="left"
    )
)

# --------------------------------------------------
# Haversine distance in KM
# --------------------------------------------------

distance_base_df = distance_base_df.withColumn(
    "seller_customer_distance_km",
    6371.0 * 2 * F.asin(
        F.sqrt(
            F.pow(
                F.sin(
                    F.radians(F.col("seller_lat") - F.col("customer_lat")) / 2
                ),
                2
            )
            +
            F.cos(F.radians(F.col("customer_lat"))) *
            F.cos(F.radians(F.col("seller_lat"))) *
            F.pow(
                F.sin(
                    F.radians(F.col("seller_lng") - F.col("customer_lng")) / 2
                ),
                2
            )
        )
    )
)

# Aggregate to one row per order
order_distance_features_df = (
    distance_base_df
    .groupBy("order_id")
    .agg(
        F.avg("seller_customer_distance_km")
            .alias("avg_seller_customer_distance_km"),

        F.max("seller_customer_distance_km")
            .alias("max_seller_customer_distance_km")
    )
)

# Join to ML dataset
ml_base_df = (
    ml_base_df
    .join(
        order_distance_features_df,
        on="order_id",
        how="left"
    )
)

print("ML rows after distance join:", ml_base_df.count())

ml_base_df.select(
    F.sum(
        F.col("avg_seller_customer_distance_km").isNull().cast("int")
    ).alias("missing_avg_distance"),

    F.sum(
        F.col("max_seller_customer_distance_km").isNull().cast("int")
    ).alias("missing_max_distance")
).show()


# In[7]:


# --------------------------------------------------
# Build payment features per order
# --------------------------------------------------

order_payment_features_df = (
    payments_df
    .groupBy("order_id")
    .agg(
        F.count("*")
            .alias("payment_count"),

        F.countDistinct("payment_type")
            .alias("payment_type_count"),

        F.sum("payment_value")
            .alias("total_payment_value"),

        F.max("payment_installments")
            .alias("max_payment_installments")
    )
)

# Join payment features to ML dataset
ml_base_df = (
    ml_base_df
    .join(
        order_payment_features_df,
        on="order_id",
        how="left"
    )
)

print("ML rows after payment join:", ml_base_df.count())

ml_base_df.select(
    F.sum(
        F.col("payment_count").isNull().cast("int")
    ).alias("missing_payment_features")
).show()


# In[8]:


# --------------------------------------------------
# Add order timing features
# --------------------------------------------------

ml_base_df = (
    ml_base_df
    .withColumn(
        "purchase_month",
        F.month("order_purchase_timestamp")
    )
    .withColumn(
        "purchase_day_of_week",
        F.dayofweek("order_purchase_timestamp")
    )
    .withColumn(
        "purchase_hour",
        F.hour("order_purchase_timestamp")
    )
    .withColumn(
        "is_weekend",
        F.when(
            F.dayofweek("order_purchase_timestamp").isin(1, 7),
            1
        ).otherwise(0)
    )
    .withColumn(
        "approval_delay_hours",
        (
            F.unix_timestamp("order_approved_at") -
            F.unix_timestamp("order_purchase_timestamp")
        ) / 3600.0
    )
    .withColumn(
        "promised_delivery_days",
        F.datediff(
            "order_estimated_delivery_date",
            "order_purchase_timestamp"
        )
    )
)

# --------------------------------------------------
# Keep exact columns expected by registered model
# --------------------------------------------------

ml_model_df = ml_base_df.select(
    "order_id",
    "order_purchase_timestamp",
    "customer_state",

    "item_count",
    "seller_count",
    "product_count",
    "total_price",
    "total_freight",
    "total_item_value",
    "avg_item_price",
    "category_count",

    "avg_product_weight_g",
    "max_product_weight_g",
    "avg_product_volume_cm3",
    "max_product_volume_cm3",
    "avg_product_photos_qty",

    "seller_state_count",
    "seller_zip_count",
    "seller_customer_same_state",

    "avg_seller_customer_distance_km",
    "max_seller_customer_distance_km",

    "payment_count",
    "payment_type_count",
    "total_payment_value",
    "max_payment_installments",

    "purchase_month",
    "purchase_day_of_week",
    "purchase_hour",
    "is_weekend",
    "approval_delay_hours",
    "promised_delivery_days",

    "label"
)

print("Final ML rows:", ml_model_df.count())
print("Final ML columns:", len(ml_model_df.columns))

ml_model_df.select(
    "order_id",
    "purchase_month",
    "purchase_day_of_week",
    "purchase_hour",
    "is_weekend",
    "approval_delay_hours",
    "promised_delivery_days",
    "label"
).show(5, truncate=False)


# In[9]:


# --------------------------------------------------
# Save refreshed ML feature dataset
# --------------------------------------------------

(
    ml_model_df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("ml_late_delivery_dataset_v1")
)

checkpoint_df = spark.table("ml_late_delivery_dataset_v1")

print("ML feature dataset refreshed successfully.")
print("Rows:", checkpoint_df.count())
print("Columns:", len(checkpoint_df.columns))


# In[ ]:




