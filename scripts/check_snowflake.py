import os

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    authenticator="SNOWFLAKE_JWT",
    private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_FILE"],
    private_key_file_pwd=os.environ["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"],
    role=os.environ["SNOWFLAKE_ROLE"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema=os.environ["SNOWFLAKE_SCHEMA"],
)

cur = conn.cursor()
cur.execute(
    "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), "
    "CURRENT_DATABASE(), CURRENT_SCHEMA()"
)
user, role, warehouse, database, schema = cur.fetchone()

print("Connected to Snowflake!")
print("User:     ", user)
print("Role:     ", role)
print("Warehouse:", warehouse)
print("Location: ", f"{database}.{schema}")

cur.close()
conn.close()
