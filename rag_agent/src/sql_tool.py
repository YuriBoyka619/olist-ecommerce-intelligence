import os
import re
from decimal import Decimal
from datetime import date, datetime

from dotenv import load_dotenv
from mssql_python import connect


load_dotenv()

SERVER = os.getenv("FABRIC_SQL_SERVER")
DATABASE = os.getenv("FABRIC_SQL_DATABASE")


# Tables that the AI agent is allowed to query
ALLOWED_TABLES = {
    "gold.dim_customer",
    "gold.dim_date",
    "gold.dim_product",
    "gold.dim_seller",
    "gold.fact_orders",
    "gold.fact_order_items",
    "gold.fact_payments",
    "gold.fact_reviews",
    "ml.late_delivery_predictions",
}


# Commands we never want the agent to execute
FORBIDDEN_KEYWORDS = [
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "ALTER",
    "TRUNCATE",
    "MERGE",
    "CREATE",
    "EXEC",
    "EXECUTE",
    "GRANT",
    "REVOKE",
    "DENY",
]


def get_connection():
    connection_string = (
        f"Server={SERVER};"
        f"Database={DATABASE};"
        "Authentication=ActiveDirectoryDeviceCode;"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )

    return connect(connection_string)


def validate_sql(query):
    query = query.strip()

    # Allow one trailing semicolon
    if query.endswith(";"):
        query = query[:-1].strip()

    # Version 1 allows SELECT queries only
    if not re.match(r"^SELECT\b", query, re.IGNORECASE):
        raise ValueError("Only SELECT queries are allowed.")

    # Prevent multiple SQL statements
    if ";" in query:
        raise ValueError("Multiple SQL statements are not allowed.")

    # Block SQL comments
    if "--" in query or "/*" in query or "*/" in query:
        raise ValueError("SQL comments are not allowed.")

    # Block destructive / privileged commands
    for keyword in FORBIDDEN_KEYWORDS:
        if re.search(rf"\b{keyword}\b", query, re.IGNORECASE):
            raise ValueError(
                f"Forbidden SQL keyword detected: {keyword}"
            )

    # Extract tables used after FROM or JOIN
    table_references = re.findall(
        r"\b(?:FROM|JOIN)\s+([A-Za-z0-9_\.\[\]]+)",
        query,
        re.IGNORECASE,
    )

    for table in table_references:
        normalized_table = (
            table.replace("[", "")
            .replace("]", "")
            .lower()
        )

        if normalized_table not in ALLOWED_TABLES:
            raise ValueError(
                f"Table not allowed: {normalized_table}"
            )

    return query


def make_serializable(value):
    if value is None:
        return None

    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, (date, datetime)):
        return value.isoformat()

    return value


def execute_sql(query, max_rows=100):
    safe_query = validate_sql(query)

    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(safe_query)

        column_names = [
            column[0]
            for column in cursor.description
        ]

        rows = cursor.fetchmany(max_rows)

        results = []

        for row in rows:
            record = {
                column_names[i]: make_serializable(row[i])
                for i in range(len(column_names))
            }

            results.append(record)

        return results

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":

    test_query = """
    SELECT TOP 5
        order_status,
        COUNT(*) AS order_count
    FROM gold.fact_orders
    GROUP BY order_status
    ORDER BY order_count DESC
    """

    results = execute_sql(test_query)

    print("\nSQL Tool Test Results:\n")

    for result in results:
        print(result)