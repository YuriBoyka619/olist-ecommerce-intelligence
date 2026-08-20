CREATE TABLE gold.dim_customer
AS
SELECT DISTINCT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM lh_olist_ecommerce_dev.silver.customers;

SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids
FROM gold.dim_customer;

CREATE TABLE gold.dim_product
AS
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


SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS null_product_ids
FROM gold.dim_product;



CREATE TABLE gold.dim_seller
AS
SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM lh_olist_ecommerce_dev.silver.sellers;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT seller_id) AS distinct_seller_ids,
    SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) AS null_seller_ids
FROM gold.dim_seller;

SELECT
    MIN(date_value) AS min_date,
    MAX(date_value) AS max_date
FROM
(
    SELECT CAST(order_purchase_timestamp AS DATE) AS date_value
    FROM lh_olist_ecommerce_dev.silver.orders

    UNION ALL

    SELECT CAST(order_approved_at AS DATE)
    FROM lh_olist_ecommerce_dev.silver.orders

    UNION ALL

    SELECT CAST(order_delivered_carrier_date AS DATE)
    FROM lh_olist_ecommerce_dev.silver.orders

    UNION ALL

    SELECT CAST(order_delivered_customer_date AS DATE)
    FROM lh_olist_ecommerce_dev.silver.orders

    UNION ALL

    SELECT CAST(order_estimated_delivery_date AS DATE)
    FROM lh_olist_ecommerce_dev.silver.orders
) d
WHERE date_value IS NOT NULL;

CREATE TABLE gold.dim_date
AS
SELECT
    YEAR(full_date) * 10000
        + MONTH(full_date) * 100
        + DAY(full_date) AS date_key,

    full_date,

    DAY(full_date) AS day,

    CASE ((DATEDIFF(DAY, '1900-01-01', full_date) % 7) + 1)
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END AS day_name,

    ((DATEDIFF(DAY, '1900-01-01', full_date) % 7) + 1) AS day_of_week,

    DATEPART(ISO_WEEK, full_date) AS week_of_year,

    MONTH(full_date) AS month,

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
    END AS month_name,

    DATEPART(QUARTER, full_date) AS quarter,

    YEAR(full_date) AS year,

    CAST(
        CONCAT(
            YEAR(full_date),
            '-',
            RIGHT('0' + CAST(MONTH(full_date) AS VARCHAR(2)), 2)
        )
        AS VARCHAR(7)
    ) AS year_month,

    CASE
        WHEN ((DATEDIFF(DAY, '1900-01-01', full_date) % 7) + 1) IN (6, 7)
        THEN 1
        ELSE 0
    END AS is_weekend

FROM
(
    SELECT
        DATEADD(DAY, value, CAST('2016-09-04' AS DATE)) AS full_date
    FROM GENERATE_SERIES(
        0,
        DATEDIFF(
            DAY,
            CAST('2016-09-04' AS DATE),
            CAST('2018-11-12' AS DATE)
        )
    )
) d;

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