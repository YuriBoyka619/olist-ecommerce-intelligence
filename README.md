# Olist E-commerce Intelligence Platform

End-to-end data, analytics, machine learning, RAG, and Agentic AI capstone project built using Microsoft Fabric, Power BI, Python, FAISS, Gemini, MLflow, and Streamlit.

## Problem

E-commerce businesses need more than descriptive reporting. They need to understand operational performance, identify delivery risks before they occur, analyze customer feedback, and interact with business data using natural language.

This project builds a complete intelligence platform around the Brazilian Olist e-commerce dataset.

## Solution

The solution combines:

- Microsoft Fabric data engineering
- Medallion Architecture: Bronze → Silver → Gold
- Fabric Warehouse and semantic modeling
- Power BI business intelligence
- Machine Learning for late-delivery prediction
- MLflow experiment tracking and model versioning
- Retrieval-Augmented Generation over customer reviews
- FAISS vector search
- Gemini LLM
- Agentic AI tool orchestration
- Streamlit conversational interface

## Architecture

```mermaid
flowchart LR
    A[Olist CSV Data] --> B[Fabric Landing Zone]
    B --> C[Bronze Lakehouse]
    C --> D[Silver Lakehouse]
    D --> E[Gold Warehouse]

    E --> F[Semantic Model]
    F --> G[Power BI Analytics]

    D --> H[ML Feature Engineering]
    H --> I[MLflow Experiments]
    I --> J[GBT Late Delivery Model]
    J --> K[ML Predictions]
    K --> E

    D --> L[RAG Review Knowledge]
    L --> M[Embeddings + FAISS]

    E --> N[Analytics Tool]
    K --> O[ML Tool]
    M --> P[RAG Tool]

    N --> Q[Agentic AI]
    O --> Q
    P --> Q

    Q --> R[Gemini]
    R --> S[Streamlit Assistant]

## Data Architecture

### Bronze
Raw Olist source data is ingested into Fabric Lakehouse Delta tables while preserving source-level information.

### Silver
Data is cleaned, typed, standardized, validated, and enriched using Dataflow Gen2 and PySpark.

### Gold
Business-ready dimensional and fact tables are created in Fabric Warehouse.

Dimensions:
- `dim_customer`
- `dim_date`
- `dim_product`
- `dim_seller`

Facts:
- `fact_orders`
- `fact_order_items`
- `fact_payments`
- `fact_reviews`

## Machine Learning

A Gradient-Boosted Trees model predicts late-delivery risk.

The ML workflow includes:

- Feature engineering
- Chronological Train / Validation / Test split
- Logistic Regression baseline
- Weighted Logistic Regression
- Random Forest
- Gradient-Boosted Trees
- Threshold optimization
- MLflow experiment tracking
- Model registration and versioning
- Batch scoring
- Prediction publishing back to Fabric Warehouse

Registered model:

`olist_late_delivery_gbt`

Prediction output:

`ml.late_delivery_predictions`

### ML Prediction Results

- Scored Orders: 12,507
- Predicted Late Orders: 3,609
- High Risk Orders: 726
- Average Late Probability: 6.92%

## Retrieval-Augmented Generation

Customer review text is transformed into a searchable knowledge base.

RAG workflow:

Customer Reviews  
→ Clean and enrich review text  
→ Generate multilingual embeddings  
→ Build FAISS vector index  
→ Retrieve relevant review evidence  
→ Generate a grounded Gemini response

The multilingual embedding model supports the Portuguese customer reviews present in the Olist dataset.

## Agentic AI

The conversational assistant dynamically selects tools depending on the business question.

Available tools include:

- Governed SQL analytics
- ML late-delivery risk predictions
- RAG customer-review retrieval

The agent can combine multiple tools in a single request.

Example question:

> Which orders are at highest late-delivery risk, how does historical delivery performance compare, and what are customers saying about late deliveries?

The final interface is implemented using Streamlit and Gemini.

## Power BI

The final Power BI report contains five pages:

1. Executive Overview
2. Sales & Order Analysis
3. Delivery & Customer Experience
4. Customer Reviews & Experience
5. ML Risk Intelligence

The report combines descriptive analytics with machine-learning risk predictions.

## Repository Structure

~~~text
olist-ecommerce-intelligence/
├── fabric/
│   ├── notebooks/
│   ├── sql/
│   ├── dataflow/
│   ├── pipelines/
│   └── semantic_model/
├── rag_agent/
│   ├── src/
│   ├── app.py
│   └── requirements.txt
├── powerbi/
├── docs/
├── .gitignore
└── README.md
~~~

## Technologies

- Microsoft Fabric
- Fabric Lakehouse
- Fabric Warehouse
- Fabric Data Factory / Pipelines
- Dataflow Gen2
- PySpark
- SQL
- Power BI
- Spark ML
- MLflow
- Python
- Sentence Transformers
- FAISS
- Google Gemini
- Streamlit
- Git and GitHub

## Security

Sensitive information is excluded from version control.

The repository does not contain:

- API keys
- `.env` files
- Fabric credentials
- Access tokens
- Virtual environments
- Generated FAISS/vector-index data

## Project Status

End-to-end implementation completed and validated.
