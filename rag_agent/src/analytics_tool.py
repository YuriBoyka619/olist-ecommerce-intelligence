from sql_tool import execute_sql


# ---------------------------------------------------------
# Tool 1: Order status summary
# ---------------------------------------------------------
def get_order_status_summary():
    """
    Returns the number of orders by order status.
    """

    query = """
    SELECT
        order_status,
        COUNT(*) AS order_count
    FROM gold.fact_orders
    GROUP BY order_status
    ORDER BY order_count DESC
    """

    return execute_sql(query)


# ---------------------------------------------------------
# Tool 2: Delivery performance
# ---------------------------------------------------------
def get_delivery_performance():
    """
    Returns overall delivery-performance KPIs.
    """

    query = """
    SELECT
        COUNT(*) AS total_orders,

        SUM(
            CASE
                WHEN is_delivered = 1 THEN 1
                ELSE 0
            END
        ) AS delivered_orders,

        SUM(
            CASE
                WHEN is_late = 1 THEN 1
                ELSE 0
            END
        ) AS late_orders,

        AVG(
            CAST(delivery_days AS FLOAT)
        ) AS average_delivery_days

    FROM gold.fact_orders
    """

    results = execute_sql(query)

    return results[0] if results else {}


# ---------------------------------------------------------
# Tool 3: Sales summary
# ---------------------------------------------------------
def get_sales_summary():
    """
    Returns overall item-level sales/value KPIs.
    """

    query = """
    SELECT
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(*) AS total_order_items,
        SUM(price) AS total_product_value,
        SUM(freight_value) AS total_freight_value,
        SUM(item_total_value) AS total_item_value

    FROM gold.fact_order_items
    """

    results = execute_sql(query)

    return results[0] if results else {}


# ---------------------------------------------------------
# Tool 4: Top product categories
# ---------------------------------------------------------
def get_category_performance(limit=10):
    """
    Returns the highest-value product categories.
    """

    limit = max(1, min(int(limit), 50))

    query = f"""
    SELECT TOP {limit}
        p.product_category_name_display AS product_category,
        COUNT(DISTINCT f.order_id) AS order_count,
        COUNT(*) AS item_count,
        SUM(f.item_total_value) AS total_item_value

    FROM gold.fact_order_items f

    INNER JOIN gold.dim_product p
        ON f.product_id = p.product_id

    GROUP BY
        p.product_category_name_display

    ORDER BY
        total_item_value DESC
    """

    return execute_sql(query)


# ---------------------------------------------------------
# Tool 5: Top sellers
# ---------------------------------------------------------
def get_seller_performance(limit=10):
    """
    Returns sellers ranked by item value.
    """

    limit = max(1, min(int(limit), 50))

    query = f"""
    SELECT TOP {limit}
        seller_id,
        COUNT(DISTINCT order_id) AS order_count,
        COUNT(*) AS item_count,
        SUM(item_total_value) AS total_item_value

    FROM gold.fact_order_items

    GROUP BY
        seller_id

    ORDER BY
        total_item_value DESC
    """

    return execute_sql(query)


# ---------------------------------------------------------
# Tool 6: Customer review summary
# ---------------------------------------------------------
def get_customer_review_summary():
    """
    Returns structured customer-review KPIs.
    """

    query = """
    SELECT
        COUNT(*) AS total_reviews,
        AVG(CAST(review_score AS FLOAT)) AS average_review_score,

        SUM(
            CASE
                WHEN review_score >= 4 THEN 1
                ELSE 0
            END
        ) AS positive_reviews,

        SUM(
            CASE
                WHEN review_score <= 2 THEN 1
                ELSE 0
            END
        ) AS negative_reviews

    FROM gold.fact_reviews
    """

    results = execute_sql(query)

    return results[0] if results else {}


# ---------------------------------------------------------
# Test analytics tools
# ---------------------------------------------------------
if __name__ == "__main__":

    print("\n========== ORDER STATUS ==========\n")
    for row in get_order_status_summary():
        print(row)

    print("\n========== DELIVERY PERFORMANCE ==========\n")
    print(get_delivery_performance())

    print("\n========== SALES SUMMARY ==========\n")
    print(get_sales_summary())

    print("\n========== TOP 5 CATEGORIES ==========\n")
    for row in get_category_performance(5):
        print(row)

    print("\n========== TOP 5 SELLERS ==========\n")
    for row in get_seller_performance(5):
        print(row)

    print("\n========== REVIEW SUMMARY ==========\n")
    print(get_customer_review_summary())