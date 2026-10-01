"""Task 1.8: Incremental load of Salesforce Accounts using a watermark."""

import json
import os
from datetime import datetime, timezone

import snowflake.connector
from dotenv import load_dotenv
from simple_salesforce import Salesforce

load_dotenv()

OBJECT_NAME = "Account"
TABLE_NAME = OBJECT_NAME.upper()
STATE_TABLE = "_EXTRACT_STATE"
SKIP_FIELD_TYPES = {"address", "location", "base64"}


def connect_salesforce():
    return Salesforce(
        consumer_key=os.environ["SF_CONSUMER_KEY"],
        consumer_secret=os.environ["SF_CONSUMER_SECRET"],
        domain=os.environ["SF_DOMAIN"],
    )


def connect_snowflake():
    return snowflake.connector.connect(
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


# ---------- Salesforce side ----------


def get_fields(sf, object_name):
    describe = getattr(sf, object_name).describe()
    return [f["name"] for f in describe["fields"] if f["type"] not in SKIP_FIELD_TYPES]


def fetch_records(sf, object_name, fields, since):
    soql = f"SELECT {', '.join(fields)} FROM {object_name}"
    if since is not None:
        # SOQL datetime literal, e.g. 2026-09-24T10:09:24Z (no quotes)
        soql += f" WHERE SystemModstamp >= {since.strftime('%Y-%m-%dT%H:%M:%SZ')}"
    soql += " ORDER BY SystemModstamp"

    records = []
    for record in sf.query_all_iter(soql, include_deleted=True):
        record.pop("attributes", None)
        records.append(record)
    return records


def to_utc(sf_timestamp):
    """'2026-09-24T10:09:24.000+0000' -> '2026-09-24 10:09:24.000000' (UTC)."""
    dt = datetime.strptime(sf_timestamp, "%Y-%m-%dT%H:%M:%S.%f%z")
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


# ---------- Snowflake side ----------


def create_tables(cur):
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            ID              VARCHAR(18)   NOT NULL,
            SYSTEM_MODSTAMP TIMESTAMP_NTZ NOT NULL,
            IS_DELETED      BOOLEAN,
            RECORD          VARIANT,
            _LOADED_AT      TIMESTAMP_NTZ DEFAULT SYSDATE()
        )
    """)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
            OBJECT_NAME          VARCHAR PRIMARY KEY,
            LAST_SYSTEM_MODSTAMP TIMESTAMP_NTZ,
            LAST_RUN_AT          TIMESTAMP_NTZ,
            LAST_ROWS_INSERTED   NUMBER
        )
    """)


def get_watermark(cur, object_name):
    cur.execute(
        f"SELECT LAST_SYSTEM_MODSTAMP FROM {STATE_TABLE} WHERE OBJECT_NAME = %s",
        (object_name,),
    )
    row = cur.fetchone()
    return row[0] if row else None


def load_records(cur, records):
    cur.execute(f"""
        CREATE OR REPLACE TEMPORARY TABLE {TABLE_NAME}_LANDING (
            ID VARCHAR, SYSTEM_MODSTAMP VARCHAR, IS_DELETED BOOLEAN, RECORD_JSON VARCHAR
        )
    """)
    rows = [
        (
            r["Id"],
            to_utc(r["SystemModstamp"]),
            bool(r.get("IsDeleted", False)),
            json.dumps(r, default=str),
        )
        for r in records
    ]
    cur.executemany(f"INSERT INTO {TABLE_NAME}_LANDING VALUES (%s, %s, %s, %s)", rows)

    # Insert only versions we don't already have (same ID + same SystemModstamp)
    cur.execute(f"""
        MERGE INTO {TABLE_NAME} t
        USING (
            SELECT ID, TO_TIMESTAMP_NTZ(SYSTEM_MODSTAMP) AS SYSTEM_MODSTAMP,
                   IS_DELETED, PARSE_JSON(RECORD_JSON) AS RECORD
            FROM {TABLE_NAME}_LANDING
        ) s
        ON t.ID = s.ID AND t.SYSTEM_MODSTAMP = s.SYSTEM_MODSTAMP
        WHEN NOT MATCHED THEN
            INSERT (ID, SYSTEM_MODSTAMP, IS_DELETED, RECORD)
            VALUES (s.ID, s.SYSTEM_MODSTAMP, s.IS_DELETED, s.RECORD)
    """)
    return cur.fetchone()[0]  # number of rows inserted


def save_watermark(cur, object_name, new_watermark, rows_inserted):
    cur.execute(
        f"""
        MERGE INTO {STATE_TABLE} t
        USING (SELECT %s AS OBJECT_NAME, TO_TIMESTAMP_NTZ(%s) AS WM, %s AS ROWS_INSERTED) s
        ON t.OBJECT_NAME = s.OBJECT_NAME
        WHEN MATCHED THEN UPDATE SET
            LAST_SYSTEM_MODSTAMP = s.WM,
            LAST_RUN_AT = SYSDATE(),
            LAST_ROWS_INSERTED = s.ROWS_INSERTED
        WHEN NOT MATCHED THEN INSERT
            (OBJECT_NAME, LAST_SYSTEM_MODSTAMP, LAST_RUN_AT, LAST_ROWS_INSERTED)
            VALUES (s.OBJECT_NAME, s.WM, SYSDATE(), s.ROWS_INSERTED)
        """,
        (object_name, new_watermark, rows_inserted),
    )


# ---------- Main flow ----------


def main():
    sf = connect_salesforce()
    conn = connect_snowflake()
    try:
        cur = conn.cursor()
        create_tables(cur)

        since = get_watermark(cur, OBJECT_NAME)
        print(f"Watermark: {since or 'none yet (full load)'}")

        fields = get_fields(sf, OBJECT_NAME)
        records = fetch_records(sf, OBJECT_NAME, fields, since)
        print(f"Fetched {len(records)} records from Salesforce")

        if not records:
            print("Nothing new. Done.")
            return

        inserted = load_records(cur, records)
        print(f"Inserted {inserted} new versions into RAW.SALESFORCE.{TABLE_NAME}")

        # Only move the watermark forward AFTER the load succeeded
        new_watermark = max(to_utc(r["SystemModstamp"]) for r in records)
        save_watermark(cur, OBJECT_NAME, new_watermark, inserted)
        print(f"Watermark saved: {new_watermark}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
