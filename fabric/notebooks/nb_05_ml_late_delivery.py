#!/usr/bin/env python
# coding: utf-8

# ## nb_05_ml_late_delivery
# 
# null

# In[1]:


# Load Silver orders table
orders_df = spark.table("silver.orders")

print("Total rows:", orders_df.count())

orders_df.printSchema()

display(orders_df.limit(5))


# In[2]:


from pyspark.sql import functions as F

# Keep only orders with a known delivery outcome
ml_orders_df = (
    orders_df
    .filter(
        (F.col("is_delivered") == True) &
        F.col("order_delivered_customer_date").isNotNull() &
        F.col("order_estimated_delivery_date").isNotNull() &
        F.col("is_late").isNotNull()
    )
)

print("Eligible ML orders:", ml_orders_df.count())

# Check target distribution
ml_orders_df.groupBy("is_late").count().orderBy("is_late").show()


# In[3]:


# Load Silver order items
order_items_df = spark.table("silver.order_items")

# Aggregate item-level data to one row per order
order_item_features_df = (
    order_items_df
    .groupBy("order_id")
    .agg(
        F.count("*").alias("item_count"),
        F.countDistinct("seller_id").alias("seller_count"),
        F.countDistinct("product_id").alias("product_count"),
        F.sum("price").alias("total_price"),
        F.sum("freight_value").alias("total_freight"),
        F.sum("item_total_value").alias("total_item_value"),
        F.avg("price").alias("avg_item_price")
    )
)

print("Order-level feature rows:", order_item_features_df.count())

display(order_item_features_df.limit(5))


# In[4]:


# Join order-level item features to eligible ML orders
ml_base_df = (
    ml_orders_df
    .join(
        order_item_features_df,
        on="order_id",
        how="left"
    )
)

print("ML rows after join:", ml_base_df.count())

# Check whether any eligible orders are missing item features
ml_base_df.select(
    F.sum(F.col("item_count").isNull().cast("int")).alias("missing_item_features")
).show()

display(ml_base_df.limit(5))


# In[5]:


# Load customer data
customers_df = spark.table("silver.customers")

# Keep only useful customer features
customer_features_df = (
    customers_df
    .select(
        "customer_id",
        "customer_state",
        "customer_zip_code_prefix"
    )
)

# Join customer features to ML base
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
    F.sum(F.col("customer_state").isNull().cast("int")).alias("missing_customer_state"),
    F.sum(F.col("customer_zip_code_prefix").isNull().cast("int")).alias("missing_customer_zip")
).show()


# In[6]:


# Load Silver products
products_df = spark.table("silver.products")

# Join order items with product attributes
item_product_df = (
    order_items_df
    .join(
        products_df.select(
            "product_id",
            "product_category_name",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
            "is_invalid_weight"
        ),
        on="product_id",
        how="left"
    )
    .withColumn(
        "product_volume_cm3",
        F.col("product_length_cm")
        * F.col("product_height_cm")
        * F.col("product_width_cm")
    )
)

# Aggregate product characteristics to order level
order_product_features_df = (
    item_product_df
    .groupBy("order_id")
    .agg(
        F.countDistinct("product_category_name").alias("category_count"),
        F.avg("product_weight_g").alias("avg_product_weight_g"),
        F.max("product_weight_g").alias("max_product_weight_g"),
        F.avg("product_volume_cm3").alias("avg_product_volume_cm3"),
        F.max("product_volume_cm3").alias("max_product_volume_cm3"),
        F.avg("product_photos_qty").alias("avg_product_photos_qty")
    )
)

print("Order product feature rows:", order_product_features_df.count())

display(order_product_features_df.limit(5))


# In[7]:


# Join product features to ML dataset
ml_base_df = (
    ml_base_df
    .join(
        order_product_features_df,
        on="order_id",
        how="left"
    )
)

print("ML rows after product join:", ml_base_df.count())

ml_base_df.select(
    F.sum(F.col("category_count").isNull().cast("int")).alias("missing_product_features")
).show()


# In[8]:


# Load Silver sellers
sellers_df = spark.table("silver.sellers")

# Join sellers to order items
item_seller_df = (
    order_items_df
    .join(
        sellers_df.select(
            "seller_id",
            "seller_state",
            "seller_zip_code_prefix"
        ),
        on="seller_id",
        how="left"
    )
)

# Aggregate seller geography to one row per order
order_seller_features_df = (
    item_seller_df
    .groupBy("order_id")
    .agg(
        F.countDistinct("seller_state").alias("seller_state_count"),
        F.countDistinct("seller_zip_code_prefix").alias("seller_zip_count"),
        F.collect_set("seller_state").alias("seller_states")
    )
)

print("Order seller feature rows:", order_seller_features_df.count())

display(order_seller_features_df.limit(5))


# In[9]:


# Join seller features to ML dataset
ml_base_df = (
    ml_base_df
    .join(
        order_seller_features_df,
        on="order_id",
        how="left"
    )
    .withColumn(
        "seller_customer_same_state",
        F.expr("array_contains(seller_states, customer_state)").cast("int")
    )
    .drop("seller_states")
)

print("ML rows after seller join:", ml_base_df.count())

ml_base_df.select(
    F.sum(F.col("seller_state_count").isNull().cast("int")).alias("missing_seller_features")
).show()

ml_base_df.groupBy("seller_customer_same_state").count().show()


# In[10]:


# Load Silver geolocation
geolocation_df = spark.table("silver.geolocation")

# Create one representative latitude/longitude per ZIP prefix
zip_geo_df = (
    geolocation_df
    .groupBy("geolocation_zip_code_prefix")
    .agg(
        F.avg("geolocation_lat").alias("lat"),
        F.avg("geolocation_lng").alias("lng")
    )
)

print("Unique ZIP prefixes:", zip_geo_df.count())

display(zip_geo_df.limit(5))


# In[11]:


# Customer ZIP coordinates
customer_geo_df = (
    zip_geo_df
    .select(
        F.col("geolocation_zip_code_prefix").alias("customer_zip_code_prefix"),
        F.col("lat").alias("customer_lat"),
        F.col("lng").alias("customer_lng")
    )
)

# Seller ZIP coordinates
seller_geo_df = (
    zip_geo_df
    .select(
        F.col("geolocation_zip_code_prefix").alias("seller_zip_code_prefix"),
        F.col("lat").alias("seller_lat"),
        F.col("lng").alias("seller_lng")
    )
)

# Customer coordinates for each ML order
order_customer_geo_df = (
    ml_base_df
    .select(
        "order_id",
        "customer_zip_code_prefix"
    )
    .join(
        customer_geo_df,
        on="customer_zip_code_prefix",
        how="left"
    )
)

# Seller coordinates
seller_location_df = (
    sellers_df
    .select(
        "seller_id",
        "seller_zip_code_prefix"
    )
    .join(
        seller_geo_df,
        on="seller_zip_code_prefix",
        how="left"
    )
)

# One row per unique seller in each order
order_seller_distance_df = (
    order_items_df
    .select("order_id", "seller_id")
    .distinct()
    .join(
        order_customer_geo_df,
        on="order_id",
        how="inner"
    )
    .join(
        seller_location_df,
        on="seller_id",
        how="left"
    )
)

# Haversine distance
lat1 = F.radians(F.col("customer_lat"))
lon1 = F.radians(F.col("customer_lng"))
lat2 = F.radians(F.col("seller_lat"))
lon2 = F.radians(F.col("seller_lng"))

a = (
    F.pow(F.sin((lat2 - lat1) / 2), 2)
    + F.cos(lat1)
    * F.cos(lat2)
    * F.pow(F.sin((lon2 - lon1) / 2), 2)
)

order_seller_distance_df = (
    order_seller_distance_df
    .withColumn(
        "seller_customer_distance_km",
        6371 * 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))
    )
)

# Aggregate to one row per order
order_distance_features_df = (
    order_seller_distance_df
    .groupBy("order_id")
    .agg(
        F.avg("seller_customer_distance_km")
            .alias("avg_seller_customer_distance_km"),
        F.max("seller_customer_distance_km")
            .alias("max_seller_customer_distance_km")
    )
)

print("Order distance feature rows:", order_distance_features_df.count())

display(order_distance_features_df.limit(5))


# In[12]:


# Join distance features to ML dataset
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


# In[13]:


# Load Silver payments
payments_df = spark.table("silver.order_payments")

# Aggregate payments to one row per order
order_payment_features_df = (
    payments_df
    .groupBy("order_id")
    .agg(
        F.count("*").alias("payment_count"),
        F.countDistinct("payment_type").alias("payment_type_count"),
        F.sum("payment_value").alias("total_payment_value"),
        F.max("payment_installments").alias("max_payment_installments")
    )
)

print("Order payment feature rows:", order_payment_features_df.count())

display(order_payment_features_df.limit(5))


# In[14]:


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


# In[15]:


# Create time-based features available at prediction time
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
            F.dayofweek("order_purchase_timestamp").isin([1, 7]),
            1
        ).otherwise(0)
    )
    .withColumn(
        "approval_delay_hours",
        (
            F.unix_timestamp("order_approved_at")
            - F.unix_timestamp("order_purchase_timestamp")
        ) / 3600
    )
    .withColumn(
        "promised_delivery_days",
        F.datediff(
            "order_estimated_delivery_date",
            "order_purchase_date"
        )
    )
)

ml_base_df.select(
    "order_id",
    "purchase_month",
    "purchase_day_of_week",
    "purchase_hour",
    "is_weekend",
    "approval_delay_hours",
    "promised_delivery_days",
    "is_late"
).show(5)


# In[16]:


# Freeze the final leakage-safe feature dataset

ml_model_df = (
    ml_base_df
    .select(
        # Keep only for tracking / chronological splitting
        "order_id",
        "order_purchase_timestamp",

        # Customer
        "customer_state",

        # Order / item features
        "item_count",
        "seller_count",
        "product_count",
        "total_price",
        "total_freight",
        "total_item_value",
        "avg_item_price",

        # Product features
        "category_count",
        "avg_product_weight_g",
        "max_product_weight_g",
        "avg_product_volume_cm3",
        "max_product_volume_cm3",
        "avg_product_photos_qty",

        # Seller / geography features
        "seller_state_count",
        "seller_zip_count",
        "seller_customer_same_state",
        "avg_seller_customer_distance_km",
        "max_seller_customer_distance_km",

        # Payment features
        "payment_count",
        "payment_type_count",
        "total_payment_value",
        "max_payment_installments",

        # Time features
        "purchase_month",
        "purchase_day_of_week",
        "purchase_hour",
        "is_weekend",
        "approval_delay_hours",
        "promised_delivery_days",

        # Target
        F.col("is_late").cast("int").alias("label")
    )
)

print("Final ML rows:", ml_model_df.count())
print("Final ML columns:", len(ml_model_df.columns))

ml_model_df.printSchema()


# In[17]:


# Check missing values in the final ML dataset

null_counts = (
    ml_model_df
    .select([
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in ml_model_df.columns
    ])
    .collect()[0]
    .asDict()
)

missing_rows = [
    (column, count)
    for column, count in null_counts.items()
    if count > 0
]

missing_df = spark.createDataFrame(
    missing_rows,
    ["column", "missing_count"]
).orderBy(F.desc("missing_count"))

display(missing_df)


# In[18]:


# Inspect time coverage before chronological splitting

ml_model_df.select(
    F.min("order_purchase_timestamp").alias("min_order_date"),
    F.max("order_purchase_timestamp").alias("max_order_date")
).show(truncate=False)

(
    ml_model_df
    .withColumn(
        "year_month",
        F.date_format("order_purchase_timestamp", "yyyy-MM")
    )
    .groupBy("year_month")
    .agg(
        F.count("*").alias("orders"),
        F.sum("label").alias("late_orders")
    )
    .withColumn(
        "late_rate_pct",
        F.round(
            F.col("late_orders") / F.col("orders") * 100,
            2
        )
    )
    .orderBy("year_month")
    .show(50, truncate=False)
)


# In[19]:


# Chronological Train / Validation / Test split

train_df = ml_model_df.filter(
    F.col("order_purchase_timestamp") < F.to_timestamp(F.lit("2018-05-01"))
)

validation_df = ml_model_df.filter(
    (F.col("order_purchase_timestamp") >= F.to_timestamp(F.lit("2018-05-01"))) &
    (F.col("order_purchase_timestamp") < F.to_timestamp(F.lit("2018-07-01")))
)

test_df = ml_model_df.filter(
    F.col("order_purchase_timestamp") >= F.to_timestamp(F.lit("2018-07-01"))
)

print("Train rows:", train_df.count())
print("Validation rows:", validation_df.count())
print("Test rows:", test_df.count())

# Check late/not-late distribution in each split
for name, df in [
    ("TRAIN", train_df),
    ("VALIDATION", validation_df),
    ("TEST", test_df)
]:
    print(f"\n{name}")
    df.groupBy("label").count().orderBy("label").show()


# In[20]:


# Columns where median imputation makes sense
median_cols = [
    "avg_product_photos_qty",
    "avg_seller_customer_distance_km",
    "max_seller_customer_distance_km",
    "avg_product_weight_g",
    "max_product_weight_g",
    "avg_product_volume_cm3",
    "max_product_volume_cm3",
    "approval_delay_hours"
]

# Calculate medians from TRAIN ONLY
train_medians = {}

for c in median_cols:
    median_value = train_df.approxQuantile(c, [0.5], 0.01)[0]
    train_medians[c] = median_value

print("Train medians:")
for c, v in train_medians.items():
    print(f"{c}: {v}")


# In[21]:


# Add missing indicators before imputation
indicator_cols = [
    "avg_product_photos_qty",
    "avg_seller_customer_distance_km",
    "max_seller_customer_distance_km",
    "avg_product_weight_g",
    "max_product_weight_g",
    "avg_product_volume_cm3",
    "max_product_volume_cm3",
    "approval_delay_hours"
]

def preprocess_missing(df):
    result = df

    # Add missing-value indicators
    for c in indicator_cols:
        result = result.withColumn(
            f"{c}_missing",
            F.col(c).isNull().cast("int")
        )

    # Apply medians learned from TRAIN only
    result = result.fillna(train_medians)

    # Payment fields: no payment record → 0
    result = result.fillna({
        "payment_count": 0,
        "payment_type_count": 0,
        "total_payment_value": 0.0,
        "max_payment_installments": 0
    })

    return result


train_clean_df = preprocess_missing(train_df)
validation_clean_df = preprocess_missing(validation_df)
test_clean_df = preprocess_missing(test_df)

print("Train rows:", train_clean_df.count())
print("Validation rows:", validation_clean_df.count())
print("Test rows:", test_clean_df.count())


# In[22]:


for name, df in [
    ("TRAIN", train_clean_df),
    ("VALIDATION", validation_clean_df),
    ("TEST", test_clean_df)
]:
    remaining_nulls = (
        df.select([
            F.sum(F.col(c).isNull().cast("int")).alias(c)
            for c in df.columns
        ])
        .collect()[0]
        .asDict()
    )

    total_nulls = sum(remaining_nulls.values())

    print(f"{name} remaining nulls: {total_nulls}")


# In[23]:


from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler

# Numeric model features
numeric_features = [
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

    # Missing-value indicators
    "avg_product_photos_qty_missing",
    "avg_seller_customer_distance_km_missing",
    "max_seller_customer_distance_km_missing",
    "avg_product_weight_g_missing",
    "max_product_weight_g_missing",
    "avg_product_volume_cm3_missing",
    "max_product_volume_cm3_missing",
    "approval_delay_hours_missing"
]

# Encode customer_state
state_indexer = StringIndexer(
    inputCol="customer_state",
    outputCol="customer_state_index",
    handleInvalid="keep"
)

state_encoder = OneHotEncoder(
    inputCol="customer_state_index",
    outputCol="customer_state_encoded"
)

# Combine all features into one vector
assembler = VectorAssembler(
    inputCols=numeric_features + ["customer_state_encoded"],
    outputCol="features"
)

preprocessing_pipeline = Pipeline(
    stages=[
        state_indexer,
        state_encoder,
        assembler
    ]
)

# IMPORTANT: fit preprocessing on TRAIN only
preprocessing_model = preprocessing_pipeline.fit(train_clean_df)

train_prepared_df = preprocessing_model.transform(train_clean_df)
validation_prepared_df = preprocessing_model.transform(validation_clean_df)
test_prepared_df = preprocessing_model.transform(test_clean_df)

print("Train prepared:", train_prepared_df.count())
print("Validation prepared:", validation_prepared_df.count())
print("Test prepared:", test_prepared_df.count())

train_prepared_df.select(
    "order_id",
    "customer_state",
    "label",
    "features"
).show(5, truncate=False)


# In[24]:


from pyspark.ml.classification import LogisticRegression

# Baseline Logistic Regression model
lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    maxIter=100
)

# Train ONLY on training data
lr_model = lr.fit(train_prepared_df)

# Predict on validation data
validation_lr_predictions = lr_model.transform(validation_prepared_df)

print("Logistic Regression training completed.")

print(
    "Validation predictions:",
    validation_lr_predictions.count()
)

validation_lr_predictions.select(
    "order_id",
    "label",
    "probability",
    "prediction"
).show(10, truncate=False)


# In[25]:


from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Confusion matrix counts
cm = (
    validation_lr_predictions
    .agg(
        F.sum(
            F.when(
                (F.col("label") == 1) & (F.col("prediction") == 1),
                1
            ).otherwise(0)
        ).alias("TP"),

        F.sum(
            F.when(
                (F.col("label") == 0) & (F.col("prediction") == 0),
                1
            ).otherwise(0)
        ).alias("TN"),

        F.sum(
            F.when(
                (F.col("label") == 0) & (F.col("prediction") == 1),
                1
            ).otherwise(0)
        ).alias("FP"),

        F.sum(
            F.when(
                (F.col("label") == 1) & (F.col("prediction") == 0),
                1
            ).otherwise(0)
        ).alias("FN")
    )
    .collect()[0]
)

tp = cm["TP"]
tn = cm["TN"]
fp = cm["FP"]
fn = cm["FN"]

precision = tp / (tp + fp) if (tp + fp) > 0 else 0
recall = tp / (tp + fn) if (tp + fn) > 0 else 0
f1 = (
    2 * precision * recall / (precision + recall)
    if (precision + recall) > 0
    else 0
)
accuracy = (tp + tn) / (tp + tn + fp + fn)

# ROC-AUC
roc_evaluator = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)

roc_auc = roc_evaluator.evaluate(validation_lr_predictions)

# PR-AUC
pr_evaluator = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderPR"
)

pr_auc = pr_evaluator.evaluate(validation_lr_predictions)

print("Confusion Matrix")
print("----------------")
print("TP:", tp)
print("TN:", tn)
print("FP:", fp)
print("FN:", fn)

print("\nValidation Metrics")
print("------------------")
print(f"Accuracy : {accuracy:.4f}")
print(f"Precision: {precision:.4f}")
print(f"Recall   : {recall:.4f}")
print(f"F1 Score : {f1:.4f}")
print(f"ROC-AUC  : {roc_auc:.4f}")
print(f"PR-AUC   : {pr_auc:.4f}")


# In[28]:


from pyspark.ml.functions import vector_to_array

# Extract probability of the positive class: Late = 1
validation_threshold_df = (
    validation_lr_predictions
    .withColumn(
        "late_probability",
        vector_to_array("probability")[1]
    )
)

thresholds = [
    0.02, 0.03, 0.04, 0.05,
    0.06, 0.07, 0.08, 0.10,
    0.12, 0.15, 0.20, 0.25,
    0.30, 0.40, 0.50
]

results = []

for threshold in thresholds:

    temp_df = validation_threshold_df.withColumn(
        "threshold_prediction",
        (F.col("late_probability") >= threshold).cast("int")
    )

    counts = (
        temp_df
        .agg(
            F.sum(
                F.when(
                    (F.col("label") == 1) &
                    (F.col("threshold_prediction") == 1), 1
                ).otherwise(0)
            ).alias("TP"),

            F.sum(
                F.when(
                    (F.col("label") == 0) &
                    (F.col("threshold_prediction") == 1), 1
                ).otherwise(0)
            ).alias("FP"),

            F.sum(
                F.when(
                    (F.col("label") == 1) &
                    (F.col("threshold_prediction") == 0), 1
                ).otherwise(0)
            ).alias("FN")
        )
        .collect()[0]
    )

    tp = counts["TP"]
    fp = counts["FP"]
    fn = counts["FN"]

    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0

    f1 = (
    2 * precision * recall / (precision + recall)
    if precision + recall else 0.0
    )

    results.append(
        (
            threshold,
            tp,
            fp,
            fn,
            precision,
            recall,
            f1
        )
    )

threshold_results_df = spark.createDataFrame(
    results,
    [
        "threshold",
        "TP",
        "FP",
        "FN",
        "precision",
        "recall",
        "f1"
    ]
)

display(
    threshold_results_df.orderBy("threshold")
)


# In[29]:


# Calculate class weights from TRAIN only

train_class_counts = {
    row["label"]: row["count"]
    for row in train_prepared_df.groupBy("label").count().collect()
}

negative_count = train_class_counts[0]
positive_count = train_class_counts[1]
total_count = negative_count + positive_count

weight_0 = total_count / (2.0 * negative_count)
weight_1 = total_count / (2.0 * positive_count)

print("Not-late count:", negative_count)
print("Late count:", positive_count)
print("Weight for not-late:", weight_0)
print("Weight for late:", weight_1)

train_weighted_df = train_prepared_df.withColumn(
    "class_weight",
    F.when(F.col("label") == 1, F.lit(weight_1))
     .otherwise(F.lit(weight_0))
)


# In[30]:


from pyspark.ml.classification import LogisticRegression

weighted_lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    weightCol="class_weight",
    maxIter=100
)

# Train on weighted training data
weighted_lr_model = weighted_lr.fit(train_weighted_df)

# Predict on validation data
validation_weighted_predictions = (
    weighted_lr_model.transform(validation_prepared_df)
)

print("Weighted Logistic Regression training completed.")
print(
    "Validation predictions:",
    validation_weighted_predictions.count()
)

validation_weighted_predictions.select(
    "order_id",
    "label",
    "probability",
    "prediction"
).show(10, truncate=False)


# In[31]:


# Evaluate Weighted Logistic Regression on validation data

cm_weighted = (
    validation_weighted_predictions
    .agg(
        F.sum(
            F.when(
                (F.col("label") == 1) & (F.col("prediction") == 1), 1
            ).otherwise(0)
        ).alias("TP"),

        F.sum(
            F.when(
                (F.col("label") == 0) & (F.col("prediction") == 0), 1
            ).otherwise(0)
        ).alias("TN"),

        F.sum(
            F.when(
                (F.col("label") == 0) & (F.col("prediction") == 1), 1
            ).otherwise(0)
        ).alias("FP"),

        F.sum(
            F.when(
                (F.col("label") == 1) & (F.col("prediction") == 0), 1
            ).otherwise(0)
        ).alias("FN")
    )
    .collect()[0]
)

tp_w = cm_weighted["TP"]
tn_w = cm_weighted["TN"]
fp_w = cm_weighted["FP"]
fn_w = cm_weighted["FN"]

precision_w = tp_w / (tp_w + fp_w) if (tp_w + fp_w) > 0 else 0.0
recall_w = tp_w / (tp_w + fn_w) if (tp_w + fn_w) > 0 else 0.0

f1_w = (
    2 * precision_w * recall_w / (precision_w + recall_w)
    if (precision_w + recall_w) > 0
    else 0.0
)

accuracy_w = (
    (tp_w + tn_w) /
    (tp_w + tn_w + fp_w + fn_w)
)

roc_auc_w = roc_evaluator.evaluate(
    validation_weighted_predictions
)

pr_auc_w = pr_evaluator.evaluate(
    validation_weighted_predictions
)

print("Weighted Logistic Regression")
print("----------------------------")
print("TP:", tp_w)
print("TN:", tn_w)
print("FP:", fp_w)
print("FN:", fn_w)

print("\nValidation Metrics")
print("------------------")
print(f"Accuracy : {accuracy_w:.4f}")
print(f"Precision: {precision_w:.4f}")
print(f"Recall   : {recall_w:.4f}")
print(f"F1 Score : {f1_w:.4f}")
print(f"ROC-AUC  : {roc_auc_w:.4f}")
print(f"PR-AUC   : {pr_auc_w:.4f}")


# In[32]:


from pyspark.ml.classification import RandomForestClassifier

rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    numTrees=100,
    maxDepth=8,
    seed=42
)

# Train only on training data
rf_model = rf.fit(train_prepared_df)

# Predict on validation data
validation_rf_predictions = rf_model.transform(
    validation_prepared_df
)

print("Random Forest training completed.")
print(
    "Validation predictions:",
    validation_rf_predictions.count()
)

validation_rf_predictions.select(
    "order_id",
    "label",
    "probability",
    "prediction"
).show(10, truncate=False)


# In[33]:


# Evaluate Random Forest on validation data

cm_rf = (
    validation_rf_predictions
    .agg(
        F.sum(
            F.when(
                (F.col("label") == 1) & (F.col("prediction") == 1), 1
            ).otherwise(0)
        ).alias("TP"),

        F.sum(
            F.when(
                (F.col("label") == 0) & (F.col("prediction") == 0), 1
            ).otherwise(0)
        ).alias("TN"),

        F.sum(
            F.when(
                (F.col("label") == 0) & (F.col("prediction") == 1), 1
            ).otherwise(0)
        ).alias("FP"),

        F.sum(
            F.when(
                (F.col("label") == 1) & (F.col("prediction") == 0), 1
            ).otherwise(0)
        ).alias("FN")
    )
    .collect()[0]
)

tp_rf = cm_rf["TP"]
tn_rf = cm_rf["TN"]
fp_rf = cm_rf["FP"]
fn_rf = cm_rf["FN"]

precision_rf = (
    tp_rf / (tp_rf + fp_rf)
    if (tp_rf + fp_rf) > 0 else 0.0
)

recall_rf = (
    tp_rf / (tp_rf + fn_rf)
    if (tp_rf + fn_rf) > 0 else 0.0
)

f1_rf = (
    2 * precision_rf * recall_rf / (precision_rf + recall_rf)
    if (precision_rf + recall_rf) > 0 else 0.0
)

accuracy_rf = (
    (tp_rf + tn_rf) /
    (tp_rf + tn_rf + fp_rf + fn_rf)
)

roc_auc_rf = roc_evaluator.evaluate(
    validation_rf_predictions
)

pr_auc_rf = pr_evaluator.evaluate(
    validation_rf_predictions
)

print("Random Forest")
print("-------------")
print("TP:", tp_rf)
print("TN:", tn_rf)
print("FP:", fp_rf)
print("FN:", fn_rf)

print("\nValidation Metrics")
print("------------------")
print(f"Accuracy : {accuracy_rf:.4f}")
print(f"Precision: {precision_rf:.4f}")
print(f"Recall   : {recall_rf:.4f}")
print(f"F1 Score : {f1_rf:.4f}")
print(f"ROC-AUC  : {roc_auc_rf:.4f}")
print(f"PR-AUC   : {pr_auc_rf:.4f}")


# In[34]:


from pyspark.ml.classification import GBTClassifier

# Gradient-Boosted Trees
gbt = GBTClassifier(
    featuresCol="features",
    labelCol="label",
    maxIter=50,
    maxDepth=5,
    stepSize=0.1,
    seed=42
)

# Train only on training data
gbt_model = gbt.fit(train_prepared_df)

# Predict on validation data
validation_gbt_predictions = gbt_model.transform(
    validation_prepared_df
)

print("GBT training completed.")
print(
    "Validation predictions:",
    validation_gbt_predictions.count()
)

validation_gbt_predictions.select(
    "order_id",
    "label",
    "probability",
    "prediction"
).show(10, truncate=False)


# In[35]:


# Evaluate GBT on validation data

cm_gbt = (
    validation_gbt_predictions
    .agg(
        F.sum(
            F.when(
                (F.col("label") == 1) & (F.col("prediction") == 1), 1
            ).otherwise(0)
        ).alias("TP"),

        F.sum(
            F.when(
                (F.col("label") == 0) & (F.col("prediction") == 0), 1
            ).otherwise(0)
        ).alias("TN"),

        F.sum(
            F.when(
                (F.col("label") == 0) & (F.col("prediction") == 1), 1
            ).otherwise(0)
        ).alias("FP"),

        F.sum(
            F.when(
                (F.col("label") == 1) & (F.col("prediction") == 0), 1
            ).otherwise(0)
        ).alias("FN")
    )
    .collect()[0]
)

tp_gbt = cm_gbt["TP"]
tn_gbt = cm_gbt["TN"]
fp_gbt = cm_gbt["FP"]
fn_gbt = cm_gbt["FN"]

precision_gbt = (
    tp_gbt / (tp_gbt + fp_gbt)
    if (tp_gbt + fp_gbt) > 0 else 0.0
)

recall_gbt = (
    tp_gbt / (tp_gbt + fn_gbt)
    if (tp_gbt + fn_gbt) > 0 else 0.0
)

f1_gbt = (
    2 * precision_gbt * recall_gbt /
    (precision_gbt + recall_gbt)
    if (precision_gbt + recall_gbt) > 0
    else 0.0
)

accuracy_gbt = (
    (tp_gbt + tn_gbt) /
    (tp_gbt + tn_gbt + fp_gbt + fn_gbt)
)

roc_auc_gbt = roc_evaluator.evaluate(
    validation_gbt_predictions
)

pr_auc_gbt = pr_evaluator.evaluate(
    validation_gbt_predictions
)

print("Gradient-Boosted Trees")
print("----------------------")
print("TP:", tp_gbt)
print("TN:", tn_gbt)
print("FP:", fp_gbt)
print("FN:", fn_gbt)

print("\nValidation Metrics")
print("------------------")
print(f"Accuracy : {accuracy_gbt:.4f}")
print(f"Precision: {precision_gbt:.4f}")
print(f"Recall   : {recall_gbt:.4f}")
print(f"F1 Score : {f1_gbt:.4f}")
print(f"ROC-AUC  : {roc_auc_gbt:.4f}")
print(f"PR-AUC   : {pr_auc_gbt:.4f}")


# In[36]:


from pyspark.ml.functions import vector_to_array

gbt_threshold_df = (
    validation_gbt_predictions
    .withColumn(
        "late_probability",
        vector_to_array("probability")[1]
    )
)

thresholds = [
    0.02, 0.03, 0.04, 0.05,
    0.06, 0.07, 0.08, 0.10,
    0.12, 0.15, 0.20, 0.25,
    0.30, 0.40, 0.50
]

gbt_results = []

for threshold in thresholds:

    temp_df = gbt_threshold_df.withColumn(
        "threshold_prediction",
        (F.col("late_probability") >= threshold).cast("int")
    )

    counts = (
        temp_df
        .agg(
            F.sum(
                F.when(
                    (F.col("label") == 1) &
                    (F.col("threshold_prediction") == 1), 1
                ).otherwise(0)
            ).alias("TP"),

            F.sum(
                F.when(
                    (F.col("label") == 0) &
                    (F.col("threshold_prediction") == 1), 1
                ).otherwise(0)
            ).alias("FP"),

            F.sum(
                F.when(
                    (F.col("label") == 1) &
                    (F.col("threshold_prediction") == 0), 1
                ).otherwise(0)
            ).alias("FN")
        )
        .collect()[0]
    )

    tp = counts["TP"]
    fp = counts["FP"]
    fn = counts["FN"]

    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0

    f1 = (
        2 * precision * recall / (precision + recall)
        if (precision + recall) else 0.0
    )

    gbt_results.append(
        (float(threshold), int(tp), int(fp), int(fn),
         float(precision), float(recall), float(f1))
    )

gbt_threshold_results_df = spark.createDataFrame(
    gbt_results,
    [
        "threshold",
        "TP",
        "FP",
        "FN",
        "precision",
        "recall",
        "f1"
    ]
)

display(
    gbt_threshold_results_df.orderBy("threshold")
)


# In[37]:


model_comparison = [
    (
        "Logistic Regression",
        0.7869,
        0.1705,
        0.15,
        0.1761,
        0.3991,
        0.2444
    ),
    (
        "Weighted Logistic Regression",
        0.7814,
        0.1629,
        0.50,
        0.1136,
        0.7058,
        0.1957
    ),
    (
        "Random Forest",
        0.6779,
        0.1152,
        0.50,
        0.0,
        0.0,
        0.0
    ),
    (
        "Gradient-Boosted Trees",
        0.7792,
        0.1521,
        0.07,
        0.1771,
        0.4304,
        0.2509
    )
]

model_comparison_df = spark.createDataFrame(
    model_comparison,
    [
        "model",
        "roc_auc",
        "pr_auc",
        "best_threshold",
        "precision",
        "recall",
        "f1"
    ]
)

display(
    model_comparison_df.orderBy(F.desc("f1"))
)


# In[38]:


from pyspark.ml.classification import GBTClassifier

gbt_configs = [
    {
        "name": "gbt_depth4",
        "maxIter": 50,
        "maxDepth": 4,
        "stepSize": 0.1
    },
    {
        "name": "gbt_depth6",
        "maxIter": 50,
        "maxDepth": 6,
        "stepSize": 0.1
    }
]

gbt_tuning_results = []

for config in gbt_configs:

    print(f"Training {config['name']}...")

    model = GBTClassifier(
        featuresCol="features",
        labelCol="label",
        maxIter=config["maxIter"],
        maxDepth=config["maxDepth"],
        stepSize=config["stepSize"],
        seed=42
    ).fit(train_prepared_df)

    predictions = model.transform(validation_prepared_df)

    roc_auc = roc_evaluator.evaluate(predictions)
    pr_auc = pr_evaluator.evaluate(predictions)

    gbt_tuning_results.append(
        (
            config["name"],
            config["maxIter"],
            config["maxDepth"],
            config["stepSize"],
            roc_auc,
            pr_auc
        )
    )

gbt_tuning_df = spark.createDataFrame(
    gbt_tuning_results,
    [
        "model",
        "max_iter",
        "max_depth",
        "step_size",
        "roc_auc",
        "pr_auc"
    ]
)

display(
    gbt_tuning_df.orderBy(F.desc("pr_auc"))
)


# In[39]:


from pyspark.ml.classification import GBTClassifier

# Final GBT candidate from tuning
gbt_final_candidate = GBTClassifier(
    featuresCol="features",
    labelCol="label",
    maxIter=50,
    maxDepth=4,
    stepSize=0.1,
    seed=42
)

gbt_final_candidate_model = gbt_final_candidate.fit(
    train_prepared_df
)

validation_gbt_final_predictions = (
    gbt_final_candidate_model.transform(
        validation_prepared_df
    )
)

print("Final GBT candidate trained.")
print(
    "Validation predictions:",
    validation_gbt_final_predictions.count()
)


# In[40]:


from pyspark.ml.functions import vector_to_array

# Extract late probability
gbt_final_threshold_df = (
    validation_gbt_final_predictions
    .withColumn(
        "late_probability",
        vector_to_array("probability")[1]
    )
)

thresholds = [
    0.03, 0.04, 0.05, 0.06,
    0.07, 0.08, 0.09, 0.10,
    0.12, 0.15, 0.20
]

results = []

for threshold in thresholds:

    temp_df = gbt_final_threshold_df.withColumn(
        "pred",
        (F.col("late_probability") >= threshold).cast("int")
    )

    counts = temp_df.agg(
        F.sum(
            F.when(
                (F.col("label") == 1) & (F.col("pred") == 1), 1
            ).otherwise(0)
        ).alias("TP"),

        F.sum(
            F.when(
                (F.col("label") == 0) & (F.col("pred") == 1), 1
            ).otherwise(0)
        ).alias("FP"),

        F.sum(
            F.when(
                (F.col("label") == 1) & (F.col("pred") == 0), 1
            ).otherwise(0)
        ).alias("FN")
    ).collect()[0]

    tp = counts["TP"]
    fp = counts["FP"]
    fn = counts["FN"]

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0

    f1 = (
        2 * precision * recall / (precision + recall)
        if (precision + recall) > 0
        else 0.0
    )

    results.append(
        (
            float(threshold),
            int(tp),
            int(fp),
            int(fn),
            float(precision),
            float(recall),
            float(f1)
        )
    )

gbt_final_threshold_results_df = spark.createDataFrame(
    results,
    [
        "threshold",
        "TP",
        "FP",
        "FN",
        "precision",
        "recall",
        "f1"
    ]
)

display(
    gbt_final_threshold_results_df.orderBy(
        F.desc("f1")
    )
)


# In[41]:


from pyspark.ml.functions import vector_to_array

# Predict on untouched TEST set
test_gbt_predictions = (
    gbt_final_candidate_model
    .transform(test_prepared_df)
    .withColumn(
        "late_probability",
        vector_to_array("probability")[1]
    )
    .withColumn(
        "final_prediction",
        (F.col("late_probability") >= 0.07).cast("int")
    )
)

# Confusion matrix
test_cm = (
    test_gbt_predictions
    .agg(
        F.sum(
            F.when(
                (F.col("label") == 1) &
                (F.col("final_prediction") == 1), 1
            ).otherwise(0)
        ).alias("TP"),

        F.sum(
            F.when(
                (F.col("label") == 0) &
                (F.col("final_prediction") == 0), 1
            ).otherwise(0)
        ).alias("TN"),

        F.sum(
            F.when(
                (F.col("label") == 0) &
                (F.col("final_prediction") == 1), 1
            ).otherwise(0)
        ).alias("FP"),

        F.sum(
            F.when(
                (F.col("label") == 1) &
                (F.col("final_prediction") == 0), 1
            ).otherwise(0)
        ).alias("FN")
    )
    .collect()[0]
)

tp_test = test_cm["TP"]
tn_test = test_cm["TN"]
fp_test = test_cm["FP"]
fn_test = test_cm["FN"]

precision_test = (
    tp_test / (tp_test + fp_test)
    if (tp_test + fp_test) > 0 else 0.0
)

recall_test = (
    tp_test / (tp_test + fn_test)
    if (tp_test + fn_test) > 0 else 0.0
)

f1_test = (
    2 * precision_test * recall_test /
    (precision_test + recall_test)
    if (precision_test + recall_test) > 0
    else 0.0
)

accuracy_test = (
    (tp_test + tn_test) /
    (tp_test + tn_test + fp_test + fn_test)
)

roc_auc_test = roc_evaluator.evaluate(
    test_gbt_predictions
)

pr_auc_test = pr_evaluator.evaluate(
    test_gbt_predictions
)

print("FINAL TEST RESULTS")
print("------------------")
print("TP:", tp_test)
print("TN:", tn_test)
print("FP:", fp_test)
print("FN:", fn_test)

print("\nMetrics")
print("-------")
print(f"Accuracy : {accuracy_test:.4f}")
print(f"Precision: {precision_test:.4f}")
print(f"Recall   : {recall_test:.4f}")
print(f"F1 Score : {f1_test:.4f}")
print(f"ROC-AUC  : {roc_auc_test:.4f}")
print(f"PR-AUC   : {pr_auc_test:.4f}")


# In[42]:


# Compare class rate across temporal splits

for name, df in [
    ("TRAIN", train_df),
    ("VALIDATION", validation_df),
    ("TEST", test_df)
]:
    stats = (
        df.agg(
            F.count("*").alias("rows"),
            F.sum("label").alias("late_orders"),
            F.avg("label").alias("late_rate")
        )
        .collect()[0]
    )

    print(
        f"{name}: "
        f"rows={stats['rows']}, "
        f"late_orders={stats['late_orders']}, "
        f"late_rate={stats['late_rate']:.4f}"
    )


# In[43]:


# Save final ML feature dataset checkpoint to Lakehouse
(
    ml_model_df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("ml_late_delivery_dataset_v1")
)

# Verify
checkpoint_df = spark.table("ml_late_delivery_dataset_v1")

print("ML checkpoint saved successfully.")
print("Rows:", checkpoint_df.count())
print("Columns:", len(checkpoint_df.columns))


# In[1]:


from pyspark.sql import functions as F

# Reload durable ML checkpoint
ml_model_df = spark.table("ml_late_delivery_dataset_v1")

print("ML checkpoint loaded successfully.")
print("Rows:", ml_model_df.count())
print("Columns:", len(ml_model_df.columns))

display(ml_model_df.limit(5))


# In[2]:


# Recreate chronological Train / Validation / Test splits

train_df = ml_model_df.filter(
    F.col("order_purchase_timestamp") < F.to_timestamp(F.lit("2018-05-01"))
)

validation_df = ml_model_df.filter(
    (F.col("order_purchase_timestamp") >= F.to_timestamp(F.lit("2018-05-01"))) &
    (F.col("order_purchase_timestamp") < F.to_timestamp(F.lit("2018-07-01")))
)

test_df = ml_model_df.filter(
    F.col("order_purchase_timestamp") >= F.to_timestamp(F.lit("2018-07-01"))
)

print("Train rows:", train_df.count())
print("Validation rows:", validation_df.count())
print("Test rows:", test_df.count())


# In[3]:


# Compare important feature averages across Train / Validation / Test

def summarize_split(name, df):
    return (
        df.agg(
            F.avg("label").alias("late_rate"),
            F.avg("promised_delivery_days").alias("avg_promised_days"),
            F.avg("approval_delay_hours").alias("avg_approval_delay_hours"),
            F.avg("avg_seller_customer_distance_km").alias("avg_distance_km"),
            F.avg("total_freight").alias("avg_freight"),
            F.avg("total_price").alias("avg_price"),
            F.avg("item_count").alias("avg_items"),
            F.avg("seller_customer_same_state").alias("same_state_rate")
        )
        .withColumn("split", F.lit(name))
    )

drift_summary_df = (
    summarize_split("TRAIN", train_df)
    .unionByName(summarize_split("VALIDATION", validation_df))
    .unionByName(summarize_split("TEST", test_df))
    .select(
        "split",
        "late_rate",
        "avg_promised_days",
        "avg_approval_delay_hours",
        "avg_distance_km",
        "avg_freight",
        "avg_price",
        "avg_items",
        "same_state_rate"
    )
)

display(drift_summary_df)


# In[4]:


# Analyze promised delivery window vs late rate across splits

def promised_days_analysis(name, df):
    return (
        df
        .withColumn(
            "promised_days_bucket",
            F.when(F.col("promised_delivery_days") <= 10, "01_<=10")
             .when(F.col("promised_delivery_days") <= 15, "02_11-15")
             .when(F.col("promised_delivery_days") <= 20, "03_16-20")
             .when(F.col("promised_delivery_days") <= 25, "04_21-25")
             .when(F.col("promised_delivery_days") <= 30, "05_26-30")
             .otherwise("06_>30")
        )
        .groupBy("promised_days_bucket")
        .agg(
            F.count("*").alias("orders"),
            F.sum("label").alias("late_orders"),
            F.avg("label").alias("late_rate")
        )
        .withColumn("split", F.lit(name))
    )

promised_drift_df = (
    promised_days_analysis("TRAIN", train_df)
    .unionByName(promised_days_analysis("VALIDATION", validation_df))
    .unionByName(promised_days_analysis("TEST", test_df))
    .select(
        "split",
        "promised_days_bucket",
        "orders",
        "late_orders",
        "late_rate"
    )
    .orderBy(
        "promised_days_bucket",
        "split"
    )
)

display(promised_drift_df)


# In[5]:


# Combine Train + Validation for final model training
development_df = (
    train_df
    .unionByName(validation_df)
)

print("Development rows:", development_df.count())

development_df.select(
    F.min("order_purchase_timestamp").alias("min_date"),
    F.max("order_purchase_timestamp").alias("max_date"),
    F.avg("label").alias("late_rate")
).show(truncate=False)


# In[6]:


# Numeric columns requiring median imputation
median_cols = [
    "avg_product_photos_qty",
    "avg_seller_customer_distance_km",
    "max_seller_customer_distance_km",
    "avg_product_weight_g",
    "max_product_weight_g",
    "avg_product_volume_cm3",
    "max_product_volume_cm3",
    "approval_delay_hours"
]

development_medians = {}

for c in median_cols:
    median_value = development_df.approxQuantile(
        c,
        [0.5],
        0.01
    )[0]

    development_medians[c] = median_value

print("Development medians:")

for c, v in development_medians.items():
    print(f"{c}: {v}")


# In[7]:


# Missing-value indicator columns
indicator_cols = [
    "avg_product_photos_qty",
    "avg_seller_customer_distance_km",
    "max_seller_customer_distance_km",
    "avg_product_weight_g",
    "max_product_weight_g",
    "avg_product_volume_cm3",
    "max_product_volume_cm3",
    "approval_delay_hours"
]

def preprocess_final_missing(df):
    result = df

    # Add missing indicators before filling nulls
    for c in indicator_cols:
        result = result.withColumn(
            f"{c}_missing",
            F.col(c).isNull().cast("int")
        )

    # Median values learned from DEVELOPMENT only
    result = result.fillna(development_medians)

    # Missing payment record means no recorded payment
    result = result.fillna({
        "payment_count": 0,
        "payment_type_count": 0,
        "total_payment_value": 0.0,
        "max_payment_installments": 0
    })

    return result


development_clean_df = preprocess_final_missing(development_df)
test_clean_df = preprocess_final_missing(test_df)

print("Development rows:", development_clean_df.count())
print("Test rows:", test_clean_df.count())

# Verify no remaining nulls
for name, df in [
    ("DEVELOPMENT", development_clean_df),
    ("TEST", test_clean_df)
]:
    null_counts = (
        df.select([
            F.sum(F.col(c).isNull().cast("int")).alias(c)
            for c in df.columns
        ])
        .collect()[0]
        .asDict()
    )

    print(
        f"{name} remaining nulls:",
        sum(null_counts.values())
    )


# In[8]:


from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler

numeric_features = [
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

    "avg_product_photos_qty_missing",
    "avg_seller_customer_distance_km_missing",
    "max_seller_customer_distance_km_missing",
    "avg_product_weight_g_missing",
    "max_product_weight_g_missing",
    "avg_product_volume_cm3_missing",
    "max_product_volume_cm3_missing",
    "approval_delay_hours_missing"
]

state_indexer = StringIndexer(
    inputCol="customer_state",
    outputCol="customer_state_index",
    handleInvalid="keep"
)

state_encoder = OneHotEncoder(
    inputCol="customer_state_index",
    outputCol="customer_state_encoded"
)

assembler = VectorAssembler(
    inputCols=numeric_features + ["customer_state_encoded"],
    outputCol="features"
)

final_preprocessing_pipeline = Pipeline(
    stages=[
        state_indexer,
        state_encoder,
        assembler
    ]
)

# Fit preprocessing on DEVELOPMENT only
final_preprocessing_model = final_preprocessing_pipeline.fit(
    development_clean_df
)

development_prepared_df = final_preprocessing_model.transform(
    development_clean_df
)

test_prepared_df = final_preprocessing_model.transform(
    test_clean_df
)

print("Development prepared:", development_prepared_df.count())
print("Test prepared:", test_prepared_df.count())


# In[9]:


from pyspark.ml.classification import GBTClassifier

final_gbt = GBTClassifier(
    featuresCol="features",
    labelCol="label",
    maxIter=50,
    maxDepth=4,
    stepSize=0.1,
    seed=42
)

final_gbt_model = final_gbt.fit(
    development_prepared_df
)

print("Final GBT model trained successfully.")
print("Training rows:", development_prepared_df.count())


# In[10]:


from pyspark.ml.functions import vector_to_array
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Score untouched TEST set
final_test_predictions = (
    final_gbt_model
    .transform(test_prepared_df)
    .withColumn(
        "late_probability",
        vector_to_array("probability")[1]
    )
    .withColumn(
        "final_prediction",
        (F.col("late_probability") >= 0.07).cast("int")
    )
)

# Confusion matrix
cm = (
    final_test_predictions
    .agg(
        F.sum(
            F.when(
                (F.col("label") == 1) &
                (F.col("final_prediction") == 1), 1
            ).otherwise(0)
        ).alias("TP"),

        F.sum(
            F.when(
                (F.col("label") == 0) &
                (F.col("final_prediction") == 0), 1
            ).otherwise(0)
        ).alias("TN"),

        F.sum(
            F.when(
                (F.col("label") == 0) &
                (F.col("final_prediction") == 1), 1
            ).otherwise(0)
        ).alias("FP"),

        F.sum(
            F.when(
                (F.col("label") == 1) &
                (F.col("final_prediction") == 0), 1
            ).otherwise(0)
        ).alias("FN")
    )
    .collect()[0]
)

tp = cm["TP"]
tn = cm["TN"]
fp = cm["FP"]
fn = cm["FN"]

precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0

f1 = (
    2 * precision * recall / (precision + recall)
    if (precision + recall) > 0 else 0.0
)

accuracy = (tp + tn) / (tp + tn + fp + fn)

roc_evaluator = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)

pr_evaluator = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderPR"
)

roc_auc = roc_evaluator.evaluate(final_test_predictions)
pr_auc = pr_evaluator.evaluate(final_test_predictions)

print("FINAL DEVELOPMENT-TRAINED GBT TEST RESULTS")
print("------------------------------------------")
print("TP:", tp)
print("TN:", tn)
print("FP:", fp)
print("FN:", fn)

print("\nMetrics")
print("-------")
print(f"Accuracy : {accuracy:.4f}")
print(f"Precision: {precision:.4f}")
print(f"Recall   : {recall:.4f}")
print(f"F1 Score : {f1:.4f}")
print(f"ROC-AUC  : {roc_auc:.4f}")
print(f"PR-AUC   : {pr_auc:.4f}")


# In[11]:


import mlflow

mlflow.set_experiment("exp_olist_late_delivery")

with mlflow.start_run(
    run_name="gbt_depth4_final_experimental"
):

    mlflow.log_params({
        "model_type": "GradientBoostedTrees",
        "max_depth": 4,
        "max_iter": 50,
        "step_size": 0.1,
        "threshold": 0.07,
        "training_rows": 83963,
        "test_rows": 12507,
        "status": "experimental_not_production_ready"
    })

    mlflow.log_metrics({
        "test_accuracy": accuracy,
        "test_precision": precision,
        "test_recall": recall,
        "test_f1": f1,
        "test_roc_auc": roc_auc,
        "test_pr_auc": pr_auc
    })

print("Final MLflow experiment logged successfully.")


# In[12]:


import mlflow
from pyspark.ml import PipelineModel

# Combine preprocessing + trained GBT into one reusable pipeline
final_full_pipeline_model = PipelineModel(
    stages=
        final_preprocessing_model.stages
        + [final_gbt_model]
)

mlflow.set_experiment("exp_olist_late_delivery")

with mlflow.start_run(
    run_name="gbt_depth4_final_model_artifact"
) as run:

    mlflow.log_params({
        "model_type": "GradientBoostedTrees",
        "max_depth": 4,
        "max_iter": 50,
        "step_size": 0.1,
        "threshold": 0.07,
        "training_rows": 83963,
        "test_rows": 12507,
        "status": "experimental_not_production_ready"
    })

    mlflow.log_metrics({
        "test_accuracy": accuracy,
        "test_precision": precision,
        "test_recall": recall,
        "test_f1": f1,
        "test_roc_auc": roc_auc,
        "test_pr_auc": pr_auc
    })

    mlflow.spark.log_model(
        final_full_pipeline_model,
        artifact_path="model"
    )

    final_model_run_id = run.info.run_id

print("Model artifact logged successfully.")
print("Run ID:", final_model_run_id)


# In[13]:


import mlflow

model_uri = f"runs:/{final_model_run_id}/model"

registered_model = mlflow.register_model(
    model_uri=model_uri,
    name="olist_late_delivery_gbt"
)

print("Model registered successfully")
print("Model name:", registered_model.name)
print("Version:", registered_model.version)


# In[14]:


import mlflow

model_info = mlflow.models.get_model_info(
    "models:/olist_late_delivery_gbt/1"
)

print("Model URI:", model_info.model_uri)
print("Signature:", model_info.signature)


# In[15]:


import mlflow.spark

loaded_model = mlflow.spark.load_model(
    "models:/olist_late_delivery_gbt/1"
)

print("Model type:", type(loaded_model))

if hasattr(loaded_model, "stages"):
    print("\nPipeline stages:")
    
    for i, stage in enumerate(loaded_model.stages):
        print(i, type(stage).__name__)
else:
    print("\nModel parameters:")
    print(loaded_model.explainParams())


# In[17]:


for i, stage in enumerate(loaded_model.stages):
    print(f"\nStage {i}: {type(stage).__name__}")

    for param_name in [
        "inputCol",
        "inputCols",
        "outputCol",
        "outputCols",
        "featuresCol",
        "labelCol"
    ]:
        if stage.hasParam(param_name):
            param = stage.getParam(param_name)

            if stage.isDefined(param):
                print(
                    f"{param_name}:",
                    stage.getOrDefault(param)
                )


# In[18]:


from pyspark.sql import DataFrame

required_cols = {
    "customer_state",
    "item_count",
    "seller_count",
    "product_count",
    "total_price",
    "total_freight",
    "total_item_value"
}

print("Possible input DataFrames:\n")

for name, obj in list(globals().items()):
    if isinstance(obj, DataFrame):
        if required_cols.issubset(set(obj.columns)):
            print(name, "->", len(obj.columns), "columns")


# In[19]:


import mlflow
import mlflow.spark

registered_model_uri = "models:/olist_late_delivery_gbt/1"

loaded_model = mlflow.spark.load_model(
    registered_model_uri
)

print("Registered model loaded successfully.")
print("Model URI:", registered_model_uri)
print("Model type:", type(loaded_model).__name__)


# In[20]:


from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

# Batch scoring using REGISTERED Fabric model
batch_predictions_df = (
    loaded_model
    .transform(test_clean_df)
    .withColumn(
        "late_probability",
        vector_to_array("probability")[1]
    )
    .withColumn(
        "predicted_late",
        (F.col("late_probability") >= 0.07).cast("int")
    )
)

batch_predictions_df.select(
    "order_id",
    "label",
    "late_probability",
    "predicted_late"
).show(10, truncate=False)

print("Batch scored rows:", batch_predictions_df.count())


# In[21]:


# Create persistent prediction output

prediction_output_df = (
    batch_predictions_df
    .select(
        "order_id",
        "late_probability",
        "predicted_late"
    )
    .withColumn(
        "risk_level",
        F.when(F.col("late_probability") >= 0.15, "High")
         .when(F.col("late_probability") >= 0.07, "Medium")
         .otherwise("Low")
    )
    .withColumn(
        "model_name",
        F.lit("olist_late_delivery_gbt")
    )
    .withColumn(
        "model_version",
        F.lit("1")
    )
    .withColumn(
        "scored_at",
        F.current_timestamp()
    )
)

(
    prediction_output_df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("ml_late_delivery_predictions")
)

print("Prediction table saved successfully.")
print(
    "Rows:",
    spark.table("ml_late_delivery_predictions").count()
)

display(
    spark.table("ml_late_delivery_predictions").limit(10)
)


# In[22]:


# Persist scoring configuration for Model Version 1

config_data = [{
    "model_name": "olist_late_delivery_gbt",
    "model_version": "1",
    "threshold": 0.07,

    "avg_product_photos_qty_median":
        float(development_medians["avg_product_photos_qty"]),

    "avg_seller_customer_distance_km_median":
        float(development_medians["avg_seller_customer_distance_km"]),

    "max_seller_customer_distance_km_median":
        float(development_medians["max_seller_customer_distance_km"]),

    "avg_product_weight_g_median":
        float(development_medians["avg_product_weight_g"]),

    "max_product_weight_g_median":
        float(development_medians["max_product_weight_g"]),

    "avg_product_volume_cm3_median":
        float(development_medians["avg_product_volume_cm3"]),

    "max_product_volume_cm3_median":
        float(development_medians["max_product_volume_cm3"]),

    "approval_delay_hours_median":
        float(development_medians["approval_delay_hours"])
}]

model_config_df = spark.createDataFrame(config_data)

(
    model_config_df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("ml_late_delivery_model_config")
)

print("Model configuration saved successfully.")

display(
    spark.table("ml_late_delivery_model_config")
)


# In[2]:


# Save model using OneLake ABFS path

model_path = (
    "abfss://Olist_Ecommerce_Intelligence_DEV_1"
    "@onelake.dfs.fabric.microsoft.com/"
    "lh_olist_ecommerce_dev.Lakehouse/"
    "Files/ml_models/olist_late_delivery_gbt/v1"
)

model_v1.write().overwrite().save(model_path)

print("Model V1 saved successfully.")
print("Path:", model_path)
print("Model type:", type(model_v1).__name__)


# In[ ]:




