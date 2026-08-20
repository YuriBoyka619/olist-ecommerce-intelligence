CREATE PROCEDURE gold.usp_load_dim_customer
AS
BEGIN

    TRUNCATE TABLE gold.dim_customer;

    INSERT INTO gold.dim_customer
    (
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    )
    SELECT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    FROM lh_olist_ecommerce_dev.silver.customers;

END;

EXEC gold.usp_load_dim_customer;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_ids
FROM gold.dim_customer;

CREATE PROCEDURE gold.usp_load_dim_product
AS
BEGIN

    TRUNCATE TABLE gold.dim_product;

    INSERT INTO gold.dim_product
    (
        product_id,
        product_category_name_english,
        product_category_name_display,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        is_invalid_weight
    )
    SELECT
        product_id,
        product_category_name_english,
        product_category_name_display,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        is_invalid_weight
    FROM lh_olist_ecommerce_dev.silver.products;

END;

EXEC gold.usp_load_dim_product;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS null_product_ids
FROM gold.dim_product;


CREATE PROCEDURE gold.usp_load_dim_seller
AS
BEGIN

    TRUNCATE TABLE gold.dim_seller;

    INSERT INTO gold.dim_seller
    (
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state
    )
    SELECT
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state
    FROM lh_olist_ecommerce_dev.silver.sellers;

END;

EXEC gold.usp_load_dim_seller;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT seller_id) AS distinct_seller_ids,
    SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) AS null_seller_ids
FROM gold.dim_seller;

CREATE PROCEDURE gold.usp_load_dim_date
AS
BEGIN

    TRUNCATE TABLE gold.dim_date;

    INSERT INTO gold.dim_date
    (
        date_key,
        full_date,
        day,
        day_name,
        day_of_week,
        week_of_year,
        month,
        month_name,
        quarter,
        year,
        year_month,
        is_weekend
    )

    SELECT
        YEAR(full_date) * 10000
            + MONTH(full_date) * 100
            + DAY(full_date) AS date_key,

        full_date,

        DAY(full_date),

        CASE ((DATEDIFF(DAY, '1900-01-01', full_date) % 7) + 1)
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
            WHEN 7 THEN 'Sunday'
        END,

        ((DATEDIFF(DAY, '1900-01-01', full_date) % 7) + 1),

        DATEPART(ISO_WEEK, full_date),

        MONTH(full_date),

        CASE MONTH(full_date)
            WHEN 1 THEN 'January'
            WHEN 2 THEN 'February'
            WHEN 3 THEN 'March'
            WHEN 4 THEN 'April'
            WHEN 5 THEN 'May'
            WHEN 6 THEN 'June'
            WHEN 7 THEN 'July'
            WHEN 8 THEN 'August'
            WHEN 9 THEN 'September'
            WHEN 10 THEN 'October'
            WHEN 11 THEN 'November'
            WHEN 12 THEN 'December'
        END,

        DATEPART(QUARTER, full_date),

        YEAR(full_date),

        CAST(
            CONCAT(
                YEAR(full_date),
                '-',
                RIGHT('0' + CAST(MONTH(full_date) AS VARCHAR(2)), 2)
            )
            AS VARCHAR(7)
        ),

        CASE
            WHEN ((DATEDIFF(DAY, '1900-01-01', full_date) % 7) + 1) IN (6, 7)
                THEN 1
            ELSE 0
        END

    FROM
    (
        SELECT
            DATEADD(
                DAY,
                value,
                CAST('2016-09-04' AS DATE)
            ) AS full_date
        FROM GENERATE_SERIES(
            0,
            DATEDIFF(
                DAY,
                CAST('2016-09-04' AS DATE),
                CAST('2018-11-12' AS DATE)
            )
        )
    ) d;

END;


EXEC gold.usp_load_dim_date;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT date_key) AS distinct_date_keys,
    COUNT(DISTINCT full_date) AS distinct_dates,
    SUM(CASE WHEN full_date IS NULL THEN 1 ELSE 0 END) AS null_dates
FROM gold.dim_date;

SELECT
    MIN(full_date) AS min_date,
    MAX(full_date) AS max_date
FROM gold.dim_date;

CREATE PROCEDURE gold.usp_load_fact_orders
AS
BEGIN

    TRUNCATE TABLE gold.fact_orders;

    INSERT INTO gold.fact_orders
    (
        order_id,
        customer_id,
        purchase_date_key,
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
    )
    SELECT
        order_id,
        customer_id,

        YEAR(order_purchase_date) * 10000
            + MONTH(order_purchase_date) * 100
            + DAY(order_purchase_date),

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

END;

EXEC gold.usp_load_fact_orders;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_ids,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_ids,
    SUM(CASE WHEN purchase_date_key IS NULL THEN 1 ELSE 0 END) AS null_purchase_date_keys
FROM gold.fact_orders;

CREATE PROCEDURE gold.usp_load_fact_order_items
AS
BEGIN

    TRUNCATE TABLE gold.fact_order_items;

    INSERT INTO gold.fact_order_items
    (
        order_id,
        order_item_id,
        customer_id,
        product_id,
        seller_id,
        purchase_date_key,
        shipping_limit_date,
        price,
        freight_value,
        item_total_value
    )




    SELECT
        oi.order_id,
        oi.order_item_id,
        o.customer_id,
        oi.product_id,
        oi.seller_id,

        YEAR(o.order_purchase_date) * 10000
            + MONTH(o.order_purchase_date) * 100
            + DAY(o.order_purchase_date),

        oi.shipping_limit_date,
        oi.price,
        oi.freight_value,
        oi.item_total_value

    FROM lh_olist_ecommerce_dev.silver.order_items oi

    INNER JOIN lh_olist_ecommerce_dev.silver.orders o
        ON oi.order_id = o.order_id;

END;

EXEC gold.usp_load_fact_order_items;

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


CREATE PROCEDURE gold.usp_load_fact_payments
AS
BEGIN

    TRUNCATE TABLE gold.fact_payments;

    INSERT INTO gold.fact_payments
    (
        order_id,
        payment_sequential,
        customer_id,
        purchase_date_key,
        payment_type,
        payment_installments,
        payment_value,
        is_invalid_installments
    )

    SELECT
        p.order_id,
        p.payment_sequential,
        o.customer_id,

        YEAR(o.order_purchase_date) * 10000
            + MONTH(o.order_purchase_date) * 100
            + DAY(o.order_purchase_date),

        p.payment_type,
        p.payment_installments,
        p.payment_value,
        p.is_invalid_installments

    FROM lh_olist_ecommerce_dev.silver.order_payments p

    INNER JOIN lh_olist_ecommerce_dev.silver.orders o
        ON p.order_id = o.order_id;

END;

EXEC gold.usp_load_fact_payments;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT CONCAT(order_id, '-', payment_sequential)) AS distinct_payments,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_ids,
    SUM(CASE WHEN payment_sequential IS NULL THEN 1 ELSE 0 END) AS null_payment_sequential,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_ids,
    SUM(CASE WHEN purchase_date_key IS NULL THEN 1 ELSE 0 END) AS null_purchase_date_keys
FROM gold.fact_payments;

CREATE PROCEDURE gold.usp_load_fact_reviews
AS
BEGIN

    TRUNCATE TABLE gold.fact_reviews;

    INSERT INTO gold.fact_reviews
    (
        review_id,
        order_id,
        customer_id,
        purchase_date_key,
        review_score,
        review_creation_date,
        review_answer_timestamp,
        has_review_title,
        has_review_comment
    )

    SELECT
        r.review_id,
        r.order_id,
        o.customer_id,

        YEAR(o.order_purchase_date) * 10000
            + MONTH(o.order_purchase_date) * 100
            + DAY(o.order_purchase_date),

        r.review_score,
        r.review_creation_date,
        r.review_answer_timestamp,

        CASE
            WHEN r.review_comment_title IS NOT NULL THEN 1
            ELSE 0
        END,

        CASE
            WHEN r.review_comment_message IS NOT NULL THEN 1
            ELSE 0
        END

    FROM lh_olist_ecommerce_dev.silver.order_reviews r

    INNER JOIN lh_olist_ecommerce_dev.silver.orders o
        ON r.order_id = o.order_id;

END;

EXEC gold.usp_load_fact_reviews;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT CONCAT(review_id, '-', order_id)) AS distinct_reviews,
    SUM(CASE WHEN review_id IS NULL THEN 1 ELSE 0 END) AS null_review_ids,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_ids,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_ids,
    SUM(CASE WHEN purchase_date_key IS NULL THEN 1 ELSE 0 END) AS null_purchase_date_keys
FROM gold.fact_reviews;

CREATE PROCEDURE gold.usp_refresh_gold
AS
BEGIN

    EXEC gold.usp_load_dim_customer;
    EXEC gold.usp_load_dim_product;
    EXEC gold.usp_load_dim_seller;
    EXEC gold.usp_load_dim_date;

    EXEC gold.usp_load_fact_orders;
    EXEC gold.usp_load_fact_order_items;
    EXEC gold.usp_load_fact_payments;
    EXEC gold.usp_load_fact_reviews;

END;

EXEC gold.usp_refresh_gold;

SELECT 'dim_customer' AS table_name, COUNT(*) AS row_count FROM gold.dim_customer
UNION ALL
SELECT 'dim_product', COUNT(*) FROM gold.dim_product
UNION ALL
SELECT 'dim_seller', COUNT(*) FROM gold.dim_seller
UNION ALL
SELECT 'dim_date', COUNT(*) FROM gold.dim_date
UNION ALL
SELECT 'fact_orders', COUNT(*) FROM gold.fact_orders
UNION ALL
SELECT 'fact_order_items', COUNT(*) FROM gold.fact_order_items
UNION ALL
SELECT 'fact_payments', COUNT(*) FROM gold.fact_payments
UNION ALL
SELECT 'fact_reviews', COUNT(*) FROM gold.fact_reviews;


SELECT 'dim_customer' AS table_name, COUNT(*) AS row_count FROM gold.dim_customer
UNION ALL
SELECT 'dim_product', COUNT(*) FROM gold.dim_product
UNION ALL
SELECT 'dim_seller', COUNT(*) FROM gold.dim_seller
UNION ALL
SELECT 'dim_date', COUNT(*) FROM gold.dim_date
UNION ALL
SELECT 'fact_orders', COUNT(*) FROM gold.fact_orders
UNION ALL
SELECT 'fact_order_items', COUNT(*) FROM gold.fact_order_items
UNION ALL
SELECT 'fact_payments', COUNT(*) FROM gold.fact_payments
UNION ALL
SELECT 'fact_reviews', COUNT(*) FROM gold.fact_reviews;