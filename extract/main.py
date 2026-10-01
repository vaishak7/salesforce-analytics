"""The pipeline: extract every Salesforce object into Snowflake RAW."""

import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from extract.salesforce_client import SalesforceClient, to_utc
from extract.snowflake_loader import SnowflakeLoader

OBJECTS = [
    "Account",
    "Contact",
    "Lead",
    "Opportunity",
    "OpportunityHistory",
    "Case",
    "Campaign",
    "User",
]

log = logging.getLogger("extract")


def setup_logging():
    Path("logs").mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        handlers=[
            logging.StreamHandler(),  # print to the terminal
            logging.FileHandler("logs/extract.log"),  # and save to a file
        ],
    )


def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def extract_object(sf, loader, object_name, run_id):
    started_at = now_utc()
    fetched = inserted = 0
    try:
        loader.create_object_table(object_name)
        since = loader.get_watermark(object_name)
        records = sf.fetch_records(object_name, since)
        fetched = len(records)

        if records:
            inserted = loader.load_records(object_name, records)
            new_watermark = max(to_utc(r["SystemModstamp"]) for r in records)
            loader.save_watermark(object_name, new_watermark, inserted)

        loader.write_audit(run_id, object_name, "SUCCESS", fetched, inserted, started_at)
        log.info("%-20s fetched=%-6d inserted=%d", object_name, fetched, inserted)

    except Exception as exc:
        log.exception("%s FAILED: %s", object_name, exc)
        try:
            loader.write_audit(
                run_id, object_name, "FAILED", fetched, inserted, started_at, str(exc)[:1000]
            )
        except Exception:
            log.exception("Could not write audit row for %s", object_name)
        raise


def main():
    setup_logging()
    run_id = uuid.uuid4().hex[:12]
    log.info("Pipeline run %s started", run_id)

    sf = SalesforceClient()
    loader = SnowflakeLoader()
    failed = []
    try:
        loader.setup()
        for object_name in OBJECTS:
            try:
                extract_object(sf, loader, object_name, run_id)
            except Exception:
                failed.append(object_name)  # keep going with the other objects
    finally:
        loader.close()

    if failed:
        log.error("Run %s finished with failures: %s", run_id, ", ".join(failed))
        return 1
    log.info("Run %s finished: all %d objects succeeded", run_id, len(OBJECTS))
    return 0


if __name__ == "__main__":
    sys.exit(main())
