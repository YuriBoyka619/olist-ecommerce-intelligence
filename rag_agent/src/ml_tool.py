from sql_tool import execute_sql
import re


# ---------------------------------------------------------
# Validate Olist order_id
# ---------------------------------------------------------
def validate_order_id(order_id: str) -> str:
    """
    Olist order IDs are 32-character hexadecimal strings.
    """
    order_id = order_id.strip().lower()

    if not re.fullmatch(r"[0-9a-f]{32}", order_id):
        raise ValueError("Invalid order_id format.")

    return order_id


# ---------------------------------------------------------
# Tool 1: Get highest-risk orders
# ---------------------------------------------------------
def get_high_risk_orders(limit: int = 10):
    """
    Returns orders with the highest late-delivery probability.
    """

    limit = max(1, min(int(limit), 100))

    query = f"""
        SELECT TOP {limit}
            order_id,
            late_probability,
            predicted_late,
            risk_level,
            model_name,
            model_version,
            scored_at
        FROM ml.late_delivery_predictions
        ORDER BY late_probability DESC;
    """

    return execute_sql(query)


# ---------------------------------------------------------
# Tool 2: Get prediction for one order
# ---------------------------------------------------------
def get_order_prediction(order_id: str):
    """
    Returns the ML prediction for a specific Olist order.
    """

    order_id = validate_order_id(order_id)

    query = f"""
        SELECT
            order_id,
            late_probability,
            predicted_late,
            risk_level,
            model_name,
            model_version,
            scored_at
        FROM ml.late_delivery_predictions
        WHERE order_id = '{order_id}';
    """

    results = execute_sql(query)

    if not results:
        return {
            "message": "No ML prediction found for this order.",
            "order_id": order_id
        }

    return results[0]


# ---------------------------------------------------------
# Tool 3: Prediction summary
# ---------------------------------------------------------
def get_prediction_summary():
    """
    Returns overall statistics for the current ML prediction snapshot.
    """

    query = """
        SELECT
            COUNT(*) AS total_scored_orders,

            SUM(
                CASE
                    WHEN predicted_late = 1 THEN 1
                    ELSE 0
                END
            ) AS predicted_late_orders,

            AVG(late_probability) AS average_late_probability,

            SUM(
                CASE
                    WHEN risk_level = 'High' THEN 1
                    ELSE 0
                END
            ) AS high_risk_orders,

            SUM(
                CASE
                    WHEN risk_level = 'Medium' THEN 1
                    ELSE 0
                END
            ) AS medium_risk_orders,

            SUM(
                CASE
                    WHEN risk_level = 'Low' THEN 1
                    ELSE 0
                END
            ) AS low_risk_orders

        FROM ml.late_delivery_predictions;
    """

    results = execute_sql(query)

    return results[0] if results else {}


# ---------------------------------------------------------
# Test the ML tool
# ---------------------------------------------------------
if __name__ == "__main__":

    print("\n========== ML PREDICTION SUMMARY ==========\n")

    summary = get_prediction_summary()
    print(summary)

    print("\n========== TOP 5 HIGH-RISK ORDERS ==========\n")

    high_risk_orders = get_high_risk_orders(5)

    for order in high_risk_orders:
        print(order)