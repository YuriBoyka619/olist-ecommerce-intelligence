#!/usr/bin/env python
# coding: utf-8

# ## nb_06_ml_scoring
# 
# null

# In[1]:


from pyspark.sql import functions as F
from pyspark.ml.functions import vector_to_array
from pyspark.ml import PipelineModel

# --------------------------------------------------
# Model information
# --------------------------------------------------

MODEL_NAME = "olist_late_delivery_gbt"
MODEL_VERSION = "1"

# --------------------------------------------------
# Load versioned Spark model from OneLake
# --------------------------------------------------

model_path = (
    "abfss://Olist_Ecommerce_Intelligence_DEV_1"
    "@onelake.dfs.fabric.microsoft.com/"
    "lh_olist_ecommerce_dev.Lakehouse/"
    "Files/ml_models/olist_late_delivery_gbt/v1"
)

loaded_model = PipelineModel.load(model_path)

# --------------------------------------------------
# Load persisted scoring configuration
# --------------------------------------------------

config_row = (
    spark.table("ml_late_delivery_model_config")
    .filter(
        (F.col("model_name") == MODEL_NAME) &
        (F.col("model_version") == MODEL_VERSION)
    )
    .first()
)

threshold = float(config_row["threshold"])

print("Scoring assets loaded successfully.")
print("Model:", MODEL_NAME)
print("Version:", MODEL_VERSION)
print("Threshold:", threshold)
print("Model type:", type(loaded_model).__name__)


# In[2]:


# --------------------------------------------------
# Load feature dataset to score
# --------------------------------------------------

scoring_df = (
    spark.table("ml_late_delivery_dataset_v1")
    .filter(
        F.col("order_purchase_timestamp") >=
        F.to_timestamp(F.lit("2018-07-01"))
    )
)

# --------------------------------------------------
# Read saved medians from model configuration
# --------------------------------------------------

median_mapping = {
    "avg_product_photos_qty":
        float(config_row["avg_product_photos_qty_median"]),

    "avg_seller_customer_distance_km":
        float(config_row["avg_seller_customer_distance_km_median"]),

    "max_seller_customer_distance_km":
        float(config_row["max_seller_customer_distance_km_median"]),

    "avg_product_weight_g":
        float(config_row["avg_product_weight_g_median"]),

    "max_product_weight_g":
        float(config_row["max_product_weight_g_median"]),

    "avg_product_volume_cm3":
        float(config_row["avg_product_volume_cm3_median"]),

    "max_product_volume_cm3":
        float(config_row["max_product_volume_cm3_median"]),

    "approval_delay_hours":
        float(config_row["approval_delay_hours_median"])
}

# --------------------------------------------------
# Add missing indicators BEFORE filling nulls
# --------------------------------------------------

for c in median_mapping.keys():
    scoring_df = scoring_df.withColumn(
        f"{c}_missing",
        F.col(c).isNull().cast("int")
    )

# Fill using TRAINING-TIME medians
scoring_df = scoring_df.fillna(median_mapping)

# Payment defaults
scoring_df = scoring_df.fillna({
    "payment_count": 0,
    "payment_type_count": 0,
    "total_payment_value": 0.0,
    "max_payment_installments": 0
})

print("Scoring data prepared successfully.")
print("Rows:", scoring_df.count())
print("Columns:", len(scoring_df.columns))


# In[3]:


# --------------------------------------------------
# Score using REGISTERED model Version 1
# --------------------------------------------------

scored_df = (
    loaded_model
    .transform(scoring_df)
    .withColumn(
        "late_probability",
        vector_to_array("probability")[1]
    )
    .withColumn(
        "predicted_late",
        (F.col("late_probability") >= threshold).cast("int")
    )
)

print("Scoring completed successfully.")
print("Scored rows:", scored_df.count())

scored_df.select(
    "order_id",
    "late_probability",
    "predicted_late"
).show(10, truncate=False)


# In[4]:


# --------------------------------------------------
# Build persistent prediction output
# --------------------------------------------------

prediction_output_df = (
    scored_df
    .select(
        "order_id",
        "late_probability",
        "predicted_late"
    )
    .withColumn(
        "risk_level",
        F.when(F.col("late_probability") >= 0.15, "High")
         .when(F.col("late_probability") >= threshold, "Medium")
         .otherwise("Low")
    )
    .withColumn(
        "model_name",
        F.lit(MODEL_NAME)
    )
    .withColumn(
        "model_version",
        F.lit(MODEL_VERSION)
    )
    .withColumn(
        "scored_at",
        F.current_timestamp()
    )
)

# --------------------------------------------------
# Save predictions
# --------------------------------------------------

(
    prediction_output_df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("ml_late_delivery_predictions")
)

print("Prediction table updated successfully.")
print(
    "Rows:",
    spark.table("ml_late_delivery_predictions").count()
)

display(
    spark.table("ml_late_delivery_predictions").limit(10)
)


# In[ ]:




