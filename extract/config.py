"""Reads settings from .env so no secrets live in the code."""

import os

from dotenv import load_dotenv

load_dotenv()


def require(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing setting in .env: {name}")
    return value


def salesforce_settings():
    return {
        "consumer_key": require("SF_CONSUMER_KEY"),
        "consumer_secret": require("SF_CONSUMER_SECRET"),
        "domain": require("SF_DOMAIN"),
    }


def snowflake_settings():
    return {
        "account": require("SNOWFLAKE_ACCOUNT"),
        "user": require("SNOWFLAKE_USER"),
        "authenticator": "SNOWFLAKE_JWT",
        "private_key_file": require("SNOWFLAKE_PRIVATE_KEY_FILE"),
        "private_key_file_pwd": require("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
        "role": require("SNOWFLAKE_ROLE"),
        "warehouse": require("SNOWFLAKE_WAREHOUSE"),
        "database": require("SNOWFLAKE_DATABASE"),
        "schema": require("SNOWFLAKE_SCHEMA"),
    }
