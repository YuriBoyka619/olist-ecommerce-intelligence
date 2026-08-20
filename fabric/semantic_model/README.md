# Semantic Model

## sm_olist_ecommerce_gold

Business-ready semantic model built on the Microsoft Fabric Gold Warehouse.

### Tables

Dimensions:
- dim_customer
- dim_date
- dim_product
- dim_seller

Facts:
- fact_orders
- fact_order_items
- fact_payments
- fact_reviews

Machine Learning:
- ml.late_delivery_predictions

### Main Relationships

- dim_customer → fact_orders
- dim_customer → fact_order_items
- dim_customer → fact_payments
- dim_customer → fact_reviews
- dim_date → fact_orders
- dim_date → fact_order_items
- dim_date → fact_payments
- dim_date → fact_reviews
- dim_product → fact_order_items
- dim_seller → fact_order_items
- fact_orders → late_delivery_predictions

Relationships use one-to-many cardinality with single-direction filtering.

### Business Measures

Measures cover:
- Orders and delivery performance
- Sales and freight
- Payments and revenue
- Customer reviews
- Customer and seller metrics
- ML late-delivery risk
