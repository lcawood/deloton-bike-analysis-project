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


def get_current_ride_data(db_connection: extensions.connection) -> int:
    """Fetched the details of the current ride from the database using  a SQL Select Query."""
    with db_connection.cursor() as db_cur:

        query = """
        SELECT first_name, last_name, height, weight, gender,
        heart_rate, power, resistance, elapsed_time
        FROM Ride
        JOIN Rider ON Ride.rider_id = Rider.rider_id
        JOIN Reading ON Ride.ride_id = Reading.ride_id
        ORDER BY start_time
        LIMIT 1
        ;
        """

        db_cur.execute(query)

        user_details = db_cur.fetchone()

        db_connection.commit()

        return user_details
