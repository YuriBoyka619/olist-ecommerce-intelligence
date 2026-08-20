SELECT
    'orders' AS table_name,
    (SELECT COUNT(*) 
     FROM lh_olist_ecommerce_dev.silver.orders) AS silver_rows,
    (SELECT COUNT(*) 
     FROM gold.fact_orders) AS gold_rows

UNION ALL

SELECT
    'order_items',
    (SELECT COUNT(*) 
     FROM lh_olist_ecommerce_dev.silver.order_items),
    (SELECT COUNT(*) 
     FROM gold.fact_order_items)

UNION ALL

SELECT
    'payments',
    (SELECT COUNT(*) 
     FROM lh_olist_ecommerce_dev.silver.order_payments),
    (SELECT COUNT(*) 
     FROM gold.fact_payments)

UNION ALL

SELECT
    'reviews',
    (SELECT COUNT(*) 
     FROM lh_olist_ecommerce_dev.silver.order_reviews),
    (SELECT COUNT(*) 
     FROM gold.fact_reviews);


SELECT
    (SELECT SUM(price)
     FROM lh_olist_ecommerce_dev.silver.order_items) AS silver_price,

    (SELECT SUM(price)
     FROM gold.fact_order_items) AS gold_price,

    (SELECT SUM(freight_value)
     FROM lh_olist_ecommerce_dev.silver.order_items) AS silver_freight,

    (SELECT SUM(freight_value)
     FROM gold.fact_order_items) AS gold_freight,

    (SELECT SUM(item_total_value)
     FROM lh_olist_ecommerce_dev.silver.order_items) AS silver_item_total,

    (SELECT SUM(item_total_value)
     FROM gold.fact_order_items) AS gold_item_total;



SELECT
    ROUND(
        (SELECT SUM(payment_value)
         FROM lh_olist_ecommerce_dev.silver.order_payments),
        2
    ) AS silver_payment_total,

    ROUND(
        (SELECT SUM(payment_value)
         FROM gold.fact_payments),
        2
    ) AS gold_payment_total;

SELECT
    (SELECT COUNT(*)
     FROM gold.fact_order_items fi
     LEFT JOIN gold.fact_orders fo
         ON fi.order_id = fo.order_id
     WHERE fo.order_id IS NULL) AS item_orders_missing,

    (SELECT COUNT(*)
     FROM gold.fact_payments fp
     LEFT JOIN gold.fact_orders fo
         ON fp.order_id = fo.order_id
     WHERE fo.order_id IS NULL) AS payment_orders_missing,

    (SELECT COUNT(*)
     FROM gold.fact_reviews fr
     LEFT JOIN gold.fact_orders fo
         ON fr.order_id = fo.order_id
     WHERE fo.order_id IS NULL) AS review_orders_missing;


SELECT
    'silver' AS layer,
    COUNT(*) AS total_orders,
    SUM(CAST(is_delivered AS INT)) AS delivered_orders,
    SUM(CAST(is_cancelled AS INT)) AS cancelled_orders,
    SUM(CAST(is_late AS INT)) AS late_orders,
    ROUND(AVG(delivery_days), 2) AS avg_delivery_days,
    ROUND(AVG(delay_days), 2) AS avg_delay_days
FROM lh_olist_ecommerce_dev.silver.orders

UNION ALL

SELECT
    'gold',
    COUNT(*),
    SUM(CAST(is_delivered AS INT)),
    SUM(CAST(is_cancelled AS INT)),
    SUM(CAST(is_late AS INT)),
    ROUND(AVG(delivery_days), 2),
    ROUND(AVG(delay_days), 2)
FROM gold.fact_orders;