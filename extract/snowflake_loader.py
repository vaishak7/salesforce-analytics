"""Everything that talks to Snowflake."""

import json
import logging

import snowflake.connector

from extract import config
from extract.salesforce_client import to_utc

log = logging.getLogger(__name__)

STATE_TABLE = "_EXTRACT_STATE"
AUDIT_TABLE = "_LOAD_AUDIT"
BATCH_SIZE = 5000


class SnowflakeLoader:
    def __init__(self):
        self.conn = snowflake.connector.connect(**config.snowflake_settings())
        self.cur = self.conn.cursor()
        log.info("Connected to Snowflake as %s", config.snowflake_settings()["user"])

    def close(self):
        self.cur.close()
        self.conn.close()

    def setup(self):
        """Create the shared state and audit tables (runs once per pipeline run)."""
        self.cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
                OBJECT_NAME          VARCHAR PRIMARY KEY,
                LAST_SYSTEM_MODSTAMP TIMESTAMP_NTZ,
                LAST_RUN_AT          TIMESTAMP_NTZ,
                LAST_ROWS_INSERTED   NUMBER
            )
        """)
        self.cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
                RUN_ID        VARCHAR,
                OBJECT_NAME   VARCHAR,
                STATUS        VARCHAR,
                ROWS_FETCHED  NUMBER,
                ROWS_INSERTED NUMBER,
                STARTED_AT    TIMESTAMP_NTZ,
                FINISHED_AT   TIMESTAMP_NTZ,
                ERROR_MESSAGE VARCHAR
            )
        """)

    def create_object_table(self, object_name):
        self.cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {object_name.upper()} (
                ID              VARCHAR(18)   NOT NULL,
                SYSTEM_MODSTAMP TIMESTAMP_NTZ NOT NULL,
                IS_DELETED      BOOLEAN,
                RECORD          VARIANT,
                _LOADED_AT      TIMESTAMP_NTZ DEFAULT SYSDATE()
            )
        """)

    def get_watermark(self, object_name):
        self.cur.execute(
            f"SELECT LAST_SYSTEM_MODSTAMP FROM {STATE_TABLE} WHERE OBJECT_NAME = %s",
            (object_name,),
        )
        row = self.cur.fetchone()
        return row[0] if row else None

    def load_records(self, object_name, records):
        table = object_name.upper()
        self.cur.execute(f"""
            CREATE OR REPLACE TEMPORARY TABLE {table}_LANDING (
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
        # Insert in batches so large objects don't build one giant statement
        for start in range(0, len(rows), BATCH_SIZE):
            batch = rows[start : start + BATCH_SIZE]
            self.cur.executemany(f"INSERT INTO {table}_LANDING VALUES (%s, %s, %s, %s)", batch)

        self.cur.execute(f"""
            MERGE INTO {table} t
            USING (
                SELECT ID, TO_TIMESTAMP_NTZ(SYSTEM_MODSTAMP) AS SYSTEM_MODSTAMP,
                       IS_DELETED, PARSE_JSON(RECORD_JSON) AS RECORD
                FROM {table}_LANDING
            ) s
            ON t.ID = s.ID AND t.SYSTEM_MODSTAMP = s.SYSTEM_MODSTAMP
            WHEN NOT MATCHED THEN
                INSERT (ID, SYSTEM_MODSTAMP, IS_DELETED, RECORD)
                VALUES (s.ID, s.SYSTEM_MODSTAMP, s.IS_DELETED, s.RECORD)
        """)
        return self.cur.fetchone()[0]

    def save_watermark(self, object_name, new_watermark, rows_inserted):
        self.cur.execute(
            f"""
            MERGE INTO {STATE_TABLE} t
            USING (SELECT %s AS OBJECT_NAME, TO_TIMESTAMP_NTZ(%s) AS WM,
                          %s AS ROWS_INSERTED) s
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

    def write_audit(self, run_id, object_name, status, fetched, inserted, started_at, error=None):
        self.cur.execute(
            f"""
            INSERT INTO {AUDIT_TABLE}
                (RUN_ID, OBJECT_NAME, STATUS, ROWS_FETCHED, ROWS_INSERTED,
                 STARTED_AT, FINISHED_AT, ERROR_MESSAGE)
            SELECT %s, %s, %s, %s, %s, TO_TIMESTAMP_NTZ(%s), SYSDATE(), %s
            """,
            (run_id, object_name, status, fetched, inserted, started_at, error),
        )
