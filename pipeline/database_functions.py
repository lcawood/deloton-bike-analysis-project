"""Module containing functions used to interact with the RDS database."""

from os import environ

from dotenv import load_dotenv
import psycopg2
from psycopg2 import extensions

load_dotenv()

def get_database_connection() -> extensions.connection:
    """Return a connection our database"""

    return psycopg2.connect(user=environ["DATABASE_USERNAME"],
                            password=environ["DATABASE_PASSWORD"],
                            host=environ["DATABASE_IP"],
                            port=environ["DATABASE_PORT"],
                            database=environ["DATABASE_NAME"]
                            )

