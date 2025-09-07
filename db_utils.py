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
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Numeric,
    Table,
    create_engine,
)

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
def define_player_peak_season(metadata):
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
        Column("birth_date", DateTime),
        Column("age", Integer),
        Column("draft_year", Integer),
        Column("draft_rnd", Integer),
        Column("draft_overall", Integer),
        Column("games_played", Integer),
        Column("time_on_ice", Numeric(4,2)),
        Column("giveaways", Integer),
        Column("shortHandedGoals", Integer),
        Column("shortHandedAssists", Integer),
        Column("blocked", Integer),
        Column("plusMinus", Integer),
        Column("evenTimeOnIce", Integer),
        Column("shortHandedTimeOnIce", Integer),
        Column("powerPlayTimeOnIce", Integer),
    )


def define_game_table(metadata):
    """Define the game table schema."""
    return Table(
        "game",
        metadata,
        Column("game_id", Integer),
        Column("season", Integer),
        Column("type", String),
        Column("date_time_GMT", DateTime),
        Column("away_team_id", Integer),
        Column("home_team_id", Integer),
        Column("away_goals", Integer),
        Column("home_goals", Integer),
        Column("outcome", String),
        Column("home_rink_side_start", String),
        Column("venue", String),
        Column("venue_link", String),
        Column("venue_time_zone_id", String),
        Column("venue_time_zone_offset", Integer),
        Column("venue_time_zone_tz", String),
    )


def define_game_shifts_table(metadata):
    """Define the table schema for game_shifts_test."""
    return Table(
        "game_shifts",
        metadata,
        Column("game_id", BigInteger),
        Column("player_id", BigInteger),
        Column("period", Integer),
        Column("shift_start", Integer),
        Column("shift_end", Integer),
    )


def define_game_plays_processor(metadata):
    """Define the schema for game_plays and return the table."""
    return Table(
        "game_plays",
        metadata,
        Column("play_id", String(20), primary_key=True),
        Column("game_id", BigInteger),
        Column("team_id_for", Integer, nullable=True),
        Column("team_id_against", Integer, nullable=True),
        Column("event", String(50)),
        Column("secondaryType", String(50)),
        Column("x", Float, nullable=True),
        Column("y", Float, nullable=True),
        Column("period", Integer),
        Column("periodType", String(50)),
        Column("periodTime", Integer),
        Column("periodTimeRemaining", Integer),
        Column("dateTime", DateTime(timezone=False)),
        Column("goals_away", Integer, nullable=True),
        Column("goals_home", Integer, nullable=True),
        Column("description", String(255)),
        Column("st_x", Integer, nullable=True),
        Column("st_y", Integer, nullable=True),
    )


def define_game_plays_players(metadata):
    """Define and return the schema for game_plays_players."""
    return Table(
        "game_plays_players",
        metadata,
        Column("play_id", String(20)),
        Column("game_id", BigInteger, nullable=False),
        Column("player_id", BigInteger, nullable=False),
        Column("playerType", String(20)),
    )


def define_player_info_table(metadata):
    """Define the player_info table schema."""
    return Table(
        "player_info",
        metadata,
        Column("player_id", BigInteger, primary_key=True),  # Unique player identifier
        Column("firstName", String(50)),
        Column("lastName", String(50)),
        Column("nationality", String(50)),
        Column("birthCity", String(50)),
        Column("primaryPosition", String(50)),
        Column("birthDate", DateTime),  # Date of birth
        Column("birthStateProvince", String(50)),
        Column("height", Float),  # Height in inches
        Column("height_cm", Float),  # Height in centimeters
        Column("weight", Float),  # Weight in pounds
        Column("shootCatches", String(10)),  # Shooting/Catching hand
    )


def create_corsi_table(table_name: str, metadata: MetaData) -> Table:
    """
    Dynamically define and return a Corsi table with the given name.

    Parameters
    ----------
    table_name : str
        The name to assign to the Corsi table.
    metadata : sqlalchemy.MetaData
        The shared metadata object.

    Returns
    -------
    sqlalchemy.Table
        SQLAlchemy table object.

    """
    return Table(
        table_name,
        metadata,
        Column("team_id", Integer),
        Column("season", Integer),
        Column("games_played", Integer),
        Column("cf", Integer),
        Column("ca", Integer),
        Column("cf_percent", Float),
        Column("capHit", Float),
    )


def create_caphit_table(table_name: str, metadata: MetaData) -> Table:
    """Define caphit table schema."""
    return Table(
        table_name,
        metadata,
        Column("firstName", String(50)),
        Column("lastName", String(50)),
        Column("capHit", Float),
    )


def create_team_event_total_games_table(table_name: str, metadata: MetaData) -> Table:
    """Define team event totals schema."""
    return Table(
        table_name,
        metadata,
        Column("team_id", Integer),
        Column("total_goals", Integer),
        Column("total_shots", Integer),
        Column("total_missed_shots", Integer),
        Column("total_blocked_shots_for", Integer),
        Column("total_goals_against", Integer),
        Column("total_shots_against", Integer),
        Column("total_missed_shots_against", Integer),
        Column("total_blocked_shots_against", Integer),
        Column("game_id", BigInteger),
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