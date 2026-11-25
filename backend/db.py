"""Postgres access for the API (short-lived connections, like the rest of the codebase). Env-configured; owns nothing from prefect_flows."""
import os

import psycopg2


def get_connection():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        client_encoding="utf8",
    )
