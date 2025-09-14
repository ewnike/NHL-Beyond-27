"""
db_utils.py.

Helper functions for database access and table creation.

Author: Eric Winiecke
Date: September 2025
"""

import logging
import os
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Numeric,
    Table,
    text,
    PrimaryKeyConstraint,
    create_engine,
)

from sqlalchemy.dialects.postgresql import ARRAY, TEXT
from log_utils import setup_logger

setup_logger()
logger = logging.getLogger(__name__)  # ✅ define logger


# 🔹 **Step 1: Load Environment Variables**
def load_environment_variables():
    """Load environment variables from `.env` file if not already set."""
    if not os.getenv("DATABASE_URL"):
        load_dotenv()
        logger.info("Environment variables loaded.")


# 🔹 **Step 2: Get Database Engine**
def get_db_engine():
    """
    Create and return a SQLAlchemy database engine.

    - Uses `DATABASE_URL` if available.
    - Otherwise, constructs a connection string from individual environment variables.

    Returns
    -------
        sqlalchemy.engine.Engine: A SQLAlchemy database engine instance.

    Raises
    ------
        ValueError: If `DATABASE_URL` is missing and required variables are not set.

    Environment Variables:
        - DATABASE_URL (optional, takes priority if set)
        - DATABASE_TYPE
        - DBAPI
        - ENDPOINT
        - USER
        - PASSWORD
        - PORT (default: 5432)
        - DATABASE

    """
    load_environment_variables()  # Ensure variables are loaded

    # Check if DATABASE_URL is set
    # pylint: disable=invalid-name
    DATABASE_URL = os.getenv("DATABASE_URL")

    if DATABASE_URL:
        logger.info("Using DATABASE_URL from environment.")
        return create_engine(DATABASE_URL)

    # Otherwise, construct from individual variables

    DATABASE_TYPE = os.getenv("DATABASE_TYPE")
    DBAPI = os.getenv("DBAPI")
    ENDPOINT = os.getenv("ENDPOINT")
    USER = os.getenv("USER")
    PASSWORD = os.getenv("PASSWORD")
    PORT = os.getenv("PORT", "5432")  # Default PostgreSQL port
    DATABASE = os.getenv("DATABASE")

    # pylint: enable=invalid-name
    # Ensure all required variables are available
    missing_vars = [
        var
        for var in ["DATABASE_TYPE", "DBAPI", "ENDPOINT", "USER", "PASSWORD", "DATABASE"]
        if not os.getenv(var)
    ]
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        raise ValueError("ERROR: One or more required environment variables are missing.")

    # URL encode the password in case it contains special characters
    encoded_password = quote_plus(PASSWORD)

    # Create the connection string
    connection_string = (
        f"{DATABASE_TYPE}+{DBAPI}://{USER}:{encoded_password}@{ENDPOINT}:{PORT}/{DATABASE}"
    )
    logger.info("Database connection string created.")

    return create_engine(connection_string)


# 🔹 **Step 3: Global MetaData Object (Prevents Duplication Issues)**
metadata = MetaData()


def get_metadata():
    """Return a global SQLAlchemy MetaData object."""
    return metadata


# 🔹 **Step 4: Table Definitions**
def define_player_peak_season(metadata, schema: str = "public"):
    """Define and return the schema for game_skater_stats."""
    return Table(
        "player_peak_season",
        metadata,
        Column("player", String),
        Column("eh_id", String),
        Column("api_id", BigInteger),
        Column("season", String),
        Column("team", String),
        Column("position", String),
        Column("shoots", String),
        Column("birthday", Date),
        Column("age", Integer),
        Column("draft_year", Integer),
        Column("draft_rnd", Integer),
        Column("draft_overall", Integer),
        Column("games_played", Integer),
        Column("time_on_ice", Numeric(8,2)),
        Column("GF%", Numeric(5,2)),
        Column("SF%", Numeric(5,2) ),
        Column("FF%", Numeric(5,2) ),
        Column("CF%", Numeric(5,2)),
        Column("xGF%", Numeric(5,2)),
        Column("GF/60", Numeric(5,2)),
        Column("GA/60", Numeric(5,2)),
        Column("SF/60", Numeric(5,2)),
        Column("SA/60", Numeric(5,2)),
        Column("FF/60", Numeric(5,2)),
        Column("FA/60", Numeric(5,2)),
        Column("CF/60", Numeric(5,2)),
        Column("CA/60", Numeric(5,2)),
        Column("xGF/60", Numeric(5,2)),
        Column("xGA/60", Numeric(5,2)),
        Column("G+-/60", Numeric(5,2)),
        Column("S+-/60", Numeric(5,2)),
        Column("F+-/60", Numeric(5,2)),
        Column("C+-/60", Numeric(5,2)),
        Column("xG+-/60", Numeric(5,2)),
        Column("Sh%", Numeric(5,2)),
        Column("Sv%", Numeric(5,2)),
        schema=schema,
    )


def define_player_five_year_aligned_z_table(metadata, schema: str = "public"):
    # Define player z_score table.
    return Table(
        "player_five_year_aligned_z_table",
        metadata,
        Column("player",String),
        Column("peak_year", Integer),
        Column("rel_age", Integer),
        Column("start_year", Integer),
        Column("season",String),
        Column("age", Integer),
        Column("cf_pct", Numeric(5,2)),
        Column("cf60", Numeric(5,2)),
        Column("ca60", Numeric(5,2)),
        Column("cf_pct_z", Numeric),
        Column("cf60_z", Numeric),
        Column("ca60_z",Numeric),
        Column("spicy_score", Numeric),
        schema=schema,
    )


def create_player_streak_seasons_table(table_name: str, metadata, schema: str = "public") -> Table:
    """
    Table of players with consecutive-season streaks.
    seasons: TEXT[] like {"13-14","14-15","15-16","16-17","17-18"}
    """
    return Table(
        table_name,
        metadata,
        Column("player", String, nullable=False),
        Column("start_year", Integer, nullable=False),  # first start year (e.g., 2013)
        Column("end_year", Integer, nullable=False),    # last start year (inclusive)
        Column("streak_len", Integer, nullable=False),  # number of consecutive seasons
        Column("seasons", ARRAY(TEXT), nullable=False),
        Column("created_at", DateTime(timezone=True), server_default=text("now()"), nullable=False),
        PrimaryKeyConstraint("player", "start_year", "end_year", name=f"pk_{table_name}"),
        schema=schema,
    )

def create_player_five_year_aligned_table(table_name: str, metadata, schema: str = "public") -> Table:
    """
    Window centered on each player's peak season with rel_age in {-2,-1,0,1,2}.
    Carries CF%, CF/60, CA/60, age, and the original 'YY-YY' season text.
    """
    return Table(
        table_name,
        metadata,
        Column("player",     String, nullable=False),
        Column("peak_year",  Integer, nullable=False),   # 4-digit start year of peak (e.g., 2017)
        Column("rel_age",    Integer, nullable=False),   # -2,-1,0,1,2 (relative to peak)
        Column("start_year", Integer, nullable=False),   # 4-digit season start year for this row
        Column("season",     String,  nullable=False),   # original 'YY-YY'
        Column("age",        Integer),
        Column("cf_pct",     Numeric(5, 2)),
        Column("cf60",       Numeric(5, 2)),
        Column("ca60",       Numeric(5, 2)),
        Column("created_at", DateTime(timezone=True), server_default=text("now()"), nullable=False),
        PrimaryKeyConstraint("player", "peak_year", "rel_age", name=f"pk_{table_name}"),
        schema=schema,
    )

def create_player_five_year_aligned_z_table(table_name: str, metadata, schema: str = "public") -> Table:
    return Table(
        table_name,
        metadata,
        Column("player",     String,  nullable=False),
        Column("peak_year",  Integer, nullable=False),
        Column("rel_age",    Integer, nullable=False),
        Column("start_year", Integer, nullable=False),
        Column("season",     String,  nullable=False),
        Column("age",        Integer),
        Column("cf_pct",     Numeric(5,2)),
        Column("cf60",       Numeric(5,2)),
        Column("ca60",       Numeric(5,2)),
        Column("cf_pct_z",   Numeric),
        Column("cf60_z",     Numeric),
        Column("ca60_z",     Numeric),
        Column("spicy_score",Numeric),
        Column("created_at", DateTime(timezone=True), server_default=text("now()"), nullable=False),
        PrimaryKeyConstraint("player","peak_year","rel_age", name=f"pk_{table_name}"),
        schema=schema,
    )

def create_table(engine, metadata, table):
    """
    Create a specific table in the database.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        The database engine.
    metadata : sqlalchemy.MetaData
        The SQLAlchemy MetaData instance.
    table : sqlalchemy.Table
        The table object to create.

    """
    metadata.create_all(engine, tables=[table])  # ✅ Create only the passed table
    logger.info(f"Table '{table.name}' created or verified.")