"""
Build a data table with the data
scraped from two sources from the Hockey Reference
website, even strength time on ice (ev_toi) and all
time on ice (all_toi). This may provide further
insight into the player caphit and skill level.

Author: Eric Winiecke.
September 27, 2025.
"""

#!/usr/bin/env python3
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from db_utils import (
    copy_csv_to_table,
    create_table,
    get_db_engine,
    get_metadata,
)
from sqlalchemy import Column, MetaData, Table, Text

# add near imports
from sqlalchemy import text as sql_text

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

CSV_PATH = Path("data/outputs/hockeyref_final.csv")
DEST_TABLE = "player_hockeyref_even_toi"  # destination table name
STAGE_TABLE = f"_{DEST_TABLE}_stage"  # staging table name
SCHEMA = "public"  # change if needed


def ensure_text_table(
    metadata: MetaData, table_name: str, columns: list[str], schema: str = "public"
) -> Table:
    """
    Create a table with all TEXT columns if it doesn't exist.
    If it exists, we leave the structure as-is (TEXT is flexible for first load).
    """
    # Build (or reflect) the table
    table = Table(
        table_name,
        metadata,
        *(Column(col, Text) for col in columns),
        schema=schema,
        extend_existing=True,
    )
    create_table(engine, metadata, table)
    return table


def main():
    if not CSV_PATH.exists():
        raise SystemExit(f"Missing CSV: {CSV_PATH}")

    # 1) Peek at the CSV header to preserve column order
    logger.info("Reading CSV header from %s", CSV_PATH)
    df_head = pd.read_csv(CSV_PATH, nrows=0)
    columns_in_order = list(df_head.columns)

    # 2) Connect + metadata
    global engine
    engine = get_db_engine()
    metadata = get_metadata()

    # 3) Create/refresh stage table (TEXT columns)
    logger.info("Ensuring stage table %s.%s", SCHEMA, STAGE_TABLE)
    stage_tbl = ensure_text_table(  # noqa: F841
        metadata, STAGE_TABLE, columns_in_order, schema=SCHEMA
    )  # noqa: F841

    # 4) Ensure schema & stage table exist, then TRUNCATE and COPY
    with engine.begin() as conn:
        # Ensure schema exists
        conn.exec_driver_sql(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')

        # Ensure stage table exists with TEXT columns (DDL in-transaction)
        cols_sql = ", ".join(f'"{c}" TEXT' for c in columns_in_order)
        conn.exec_driver_sql(f'CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{STAGE_TABLE}" ({cols_sql})')

        # Safety: verify existence (optional debug)
        exists = conn.execute(
            sql_text("SELECT to_regclass(:qn)"),
            {"qn": f'{SCHEMA}."{STAGE_TABLE}"'},
        ).scalar()
        if exists is None:
            raise RuntimeError(f"Stage table not found after CREATE: {SCHEMA}.{STAGE_TABLE}")

        logger.info("Truncating stage table %s.%s", SCHEMA, STAGE_TABLE)
        conn.exec_driver_sql(f'TRUNCATE TABLE "{SCHEMA}"."{STAGE_TABLE}"')

        logger.info("COPYing CSV into %s.%s", SCHEMA, STAGE_TABLE)
        copy_csv_to_table(
            conn.connection,
            table=STAGE_TABLE,
            csv_path=str(CSV_PATH),
            columns_in_order=columns_in_order,
            schema=SCHEMA,
        )

    # 5) Create/refresh destination table (TEXT columns)
    logger.info("Ensuring destination table %s.%s", SCHEMA, DEST_TABLE)
    dest_tbl = ensure_text_table(  # noqa: F841
        metadata, DEST_TABLE, columns_in_order, schema=SCHEMA
    )  # noqa: F841

    # 6) Replace destination data from stage
    with engine.begin() as conn:
        logger.info("Replacing %s.%s from %s.%s", SCHEMA, DEST_TABLE, SCHEMA, STAGE_TABLE)
        conn.exec_driver_sql(f'TRUNCATE TABLE "{SCHEMA}"."{DEST_TABLE}"')
        ins_cols = ", ".join(f'"{c}"' for c in columns_in_order)
        conn.exec_driver_sql(
            f'INSERT INTO "{SCHEMA}"."{DEST_TABLE}" ({ins_cols}) '
            f'SELECT {ins_cols} FROM "{SCHEMA}"."{STAGE_TABLE}"'
        )

    # 7) Export the destination table back to CSV in outputs/
    export_path = Path("data/outputs") / f"{DEST_TABLE}.csv"
    logger.info("Exporting %s.%s to %s", SCHEMA, DEST_TABLE, export_path)
    with engine.begin() as conn:
        df_out = pd.read_sql(f'SELECT * FROM "{SCHEMA}"."{DEST_TABLE}"', conn)
    export_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(export_path, index=False)

    logger.info("Done. Rows now in %s.%s will match %s", SCHEMA, DEST_TABLE, CSV_PATH)


if __name__ == "__main__":
    main()
