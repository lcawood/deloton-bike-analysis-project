"""Utility functions to interact with the RDS database."""

from os import environ


from dotenv import load_dotenv
from pandas import DataFrame
import psycopg2
from psycopg2 import extensions, OperationalError
import streamlit as st


def get_database_connection() -> extensions.connection:
    """Returns a live database connection."""

    try:
        conn = psycopg2.connect(user=environ["DATABASE_USERNAME"],
                                password=environ["DATABASE_PASSWORD"],
                                host=environ["DATABASE_IP"],
                                port=environ["DATABASE_PORT"],
                                database=environ["DATABASE_NAME"]
                                )
        return conn

    except OperationalError as err:
        print("Error connecting to database. %s", err)
        return None
