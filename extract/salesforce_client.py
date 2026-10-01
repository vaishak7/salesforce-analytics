"""Everything that talks to Salesforce."""

import logging
from datetime import datetime, timezone

from simple_salesforce import Salesforce

from extract import config

log = logging.getLogger(__name__)

SKIP_FIELD_TYPES = {"address", "location", "base64"}


def to_utc(sf_timestamp):
    """'2026-09-24T10:09:24.000+0000' -> '2026-09-24 10:09:24.000000' (UTC)."""
    dt = datetime.strptime(sf_timestamp, "%Y-%m-%dT%H:%M:%S.%f%z")
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


class SalesforceClient:
    def __init__(self):
        self.sf = Salesforce(**config.salesforce_settings())
        log.info("Connected to Salesforce: %s", self.sf.sf_instance)

    def get_fields(self, object_name):
        describe = getattr(self.sf, object_name).describe()
        return [f["name"] for f in describe["fields"] if f["type"] not in SKIP_FIELD_TYPES]

    def fetch_records(self, object_name, since):
        fields = self.get_fields(object_name)
        soql = f"SELECT {', '.join(fields)} FROM {object_name}"
        if since is not None:
            soql += f" WHERE SystemModstamp >= {since.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        soql += " ORDER BY SystemModstamp"

        records = []
        for record in self.sf.query_all_iter(soql, include_deleted=True):
            record.pop("attributes", None)
            records.append(record)
        return records