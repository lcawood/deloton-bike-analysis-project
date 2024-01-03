"""Module containing functions used to interact with the RDS database."""

from os import environ

from dotenv import load_dotenv
import psycopg2
from psycopg2 import OperationalError


def get_db_connection():
    '''Returns a connection to the postgres database.'''

    conn = None
    try:
        conn = psycopg2.connect(
            database=environ['db_name'],
            user=environ['db_user'],
            password=environ['db_password'],
            host=environ['db_host'],
            port=environ['db_port']
        )
    except OperationalError as e:
        print(f"The error '{e}' occurred.")
    finally:
        return conn


def get_user_by_id(user_id: int) -> dict:
    """Returns user in User table with given user_id; return None if no match found."""


if __name__ == "__main__":

    load_dotenv()
    get_db_connection()
