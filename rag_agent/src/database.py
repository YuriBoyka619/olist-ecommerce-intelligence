import os
from dotenv import load_dotenv
from mssql_python import connect

load_dotenv()

SERVER = os.getenv("FABRIC_SQL_SERVER")
DATABASE = os.getenv("FABRIC_SQL_DATABASE")

connection_string = (
    f"Server={SERVER};"
    f"Database={DATABASE};"
    "Authentication=ActiveDirectoryDeviceCode;"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
)

print("Connecting to Fabric Warehouse...")

conn = connect(connection_string)

cursor = conn.cursor()

cursor.execute("""
SELECT TOP 10
    TABLE_SCHEMA,
    TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_SCHEMA, TABLE_NAME;
""")

rows = cursor.fetchall()

print("\nConnected successfully. ✅")
print("\nTables found:")

for row in rows:
    print(row[0], ".", row[1])

cursor.close()
conn.close()