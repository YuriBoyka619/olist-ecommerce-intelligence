# Microsoft Fabric Pipelines

## pl_01_bronze_full_load
Orchestrates ingestion of the Olist source data into the Bronze layer.

## pl_02_silver_transformations
Orchestrates the Silver-layer transformation process after Bronze ingestion.

## pl_03_ml_prediction_publish
Publishes the latest ML late-delivery predictions from the Lakehouse to the Gold Warehouse.

Flow:
Lakehouse ml_late_delivery_predictions
→ truncate existing Warehouse prediction table
→ copy latest predictions
→ Warehouse ml.late_delivery_predictions
