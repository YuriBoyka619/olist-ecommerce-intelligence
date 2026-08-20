SELECT TOP 10 *
FROM lh_olist_ecommerce_dev.silver.orders;

CREATE TABLE gold.fact_orders
AS
SELECT
    order_id,
    customer_id,

    YEAR(order_purchase_date) * 10000
        + MONTH(order_purchase_date) * 100
        + DAY(order_purchase_date) AS purchase_date_key,

    order_status,

    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,

    delivery_days,
    delay_days,

    is_delivered,
    is_cancelled,
    is_late

FROM lh_olist_ecommerce_dev.silver.orders;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_ids,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_ids,
    SUM(CASE WHEN purchase_date_key IS NULL THEN 1 ELSE 0 END) AS null_purchase_date_keys
FROM gold.fact_orders;

SELECT
    SUM(CASE WHEN dc.customer_id IS NULL THEN 1 ELSE 0 END) AS missing_customers,
    SUM(CASE WHEN dd.date_key IS NULL THEN 1 ELSE 0 END) AS missing_dates
FROM gold.fact_orders f
LEFT JOIN gold.dim_customer dc
    ON f.customer_id = dc.customer_id
LEFT JOIN gold.dim_date dd
    ON f.purchase_date_key = dd.date_key;


SELECT TOP 10 *
FROM lh_olist_ecommerce_dev.silver.order_items;


CREATE TABLE gold.fact_order_items
AS
SELECT
    oi.order_id,
    oi.order_item_id,

    o.customer_id,
    oi.product_id,
    oi.seller_id,

    YEAR(o.order_purchase_date) * 10000
        + MONTH(o.order_purchase_date) * 100
        + DAY(o.order_purchase_date) AS purchase_date_key,

    oi.shipping_limit_date,

    oi.price,
    oi.freight_value,
    oi.item_total_value

FROM lh_olist_ecommerce_dev.silver.order_items oi

INNER JOIN lh_olist_ecommerce_dev.silver.orders o
    ON oi.order_id = o.order_id;


SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT CONCAT(order_id, '-', order_item_id)) AS distinct_order_items,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_ids,
    SUM(CASE WHEN order_item_id IS NULL THEN 1 ELSE 0 END) AS null_order_item_ids,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_ids,
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS null_product_ids,
    SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) AS null_seller_ids,
    SUM(CASE WHEN purchase_date_key IS NULL THEN 1 ELSE 0 END) AS null_purchase_date_keys
FROM gold.fact_order_items;

SELECT TOP 10 *
FROM lh_olist_ecommerce_dev.silver.order_payments;


CREATE TABLE gold.fact_payments
AS
SELECT
    p.order_id,
    p.payment_sequential,

    o.customer_id,

    YEAR(o.order_purchase_date) * 10000
        + MONTH(o.order_purchase_date) * 100
        + DAY(o.order_purchase_date) AS purchase_date_key,

    p.payment_type,
    p.payment_installments,
    p.payment_value,
    p.is_invalid_installments

FROM lh_olist_ecommerce_dev.silver.order_payments p

INNER JOIN lh_olist_ecommerce_dev.silver.orders o
    ON p.order_id = o.order_id;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT CONCAT(order_id, '-', payment_sequential)) AS distinct_payments,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_ids,
    SUM(CASE WHEN payment_sequential IS NULL THEN 1 ELSE 0 END) AS null_payment_sequential,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_ids,
    SUM(CASE WHEN purchase_date_key IS NULL THEN 1 ELSE 0 END) AS null_purchase_date_keys
FROM gold.fact_payments;

SELECT
    SUM(CASE WHEN dc.customer_id IS NULL THEN 1 ELSE 0 END) AS missing_customers,
    SUM(CASE WHEN dd.date_key IS NULL THEN 1 ELSE 0 END) AS missing_dates
FROM gold.fact_payments f

LEFT JOIN gold.dim_customer dc
    ON f.customer_id = dc.customer_id

LEFT JOIN gold.dim_date dd
    ON f.purchase_date_key = dd.date_key;


SELECT TOP 10 *
FROM lh_olist_ecommerce_dev.silver.order_reviews;

DROP TABLE IF EXISTS gold.fact_reviews;

CREATE TABLE gold.fact_reviews
AS
SELECT
    r.review_id,
    r.order_id,

    o.customer_id,

    YEAR(o.order_purchase_date) * 10000
        + MONTH(o.order_purchase_date) * 100
        + DAY(o.order_purchase_date) AS purchase_date_key,

    r.review_score,
    r.review_creation_date,
    r.review_answer_timestamp,

    CASE
        WHEN r.review_comment_title IS NOT NULL THEN 1
        ELSE 0
    END AS has_review_title,

    CASE
        WHEN r.review_comment_message IS NOT NULL THEN 1
        ELSE 0
    END AS has_review_comment

FROM lh_olist_ecommerce_dev.silver.order_reviews r

INNER JOIN lh_olist_ecommerce_dev.silver.orders o
    ON r.order_id = o.order_id;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT CONCAT(review_id, '-', order_id)) AS distinct_reviews,
    SUM(CASE WHEN review_id IS NULL THEN 1 ELSE 0 END) AS null_review_ids,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_ids,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_ids,
    SUM(CASE WHEN purchase_date_key IS NULL THEN 1 ELSE 0 END) AS null_purchase_date_keys
FROM gold.fact_reviews;

SELECT
    SUM(CASE WHEN dc.customer_id IS NULL THEN 1 ELSE 0 END) AS missing_customers,
    SUM(CASE WHEN dd.date_key IS NULL THEN 1 ELSE 0 END) AS missing_dates
FROM gold.fact_reviews f

LEFT JOIN gold.dim_customer dc
    ON f.customer_id = dc.customer_id

LEFT JOIN gold.dim_date dd
    ON f.purchase_date_key = dd.date_key;