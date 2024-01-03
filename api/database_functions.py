"""Module containing functions used to interact with the RDS database."""

from datetime import datetime
from os import environ

from dotenv import load_dotenv
import psycopg2
from psycopg2.extensions import connection


load_dotenv()


def get_database_connection() -> connection:
    """Return a connection our database."""

    return psycopg2.connect(user=environ["DATABASE_USERNAME"],
                            password=environ["DATABASE_PASSWORD"],
                            host=environ["DATABASE_IP"],
                            port=environ["DATABASE_PORT"],
                            database=environ["DATABASE_NAME"]
                            )


def get_ride_by_id(db_connection: connection, ride_id: int) -> dict:
    """
    Returns dictionary of entry in Ride table with ride_id matching that given; returns None if no
    matches are found.
    """

    with db_connection.cursor() as db_cur:

        query = """SELECT * FROM Ride WHERE ride_id=%s"""
        parameters = (ride_id)
        db_cur.execute(query,parameters)

        ride = db_cur.fetchone()

        if ride:
            return dict(ride)

        return None
    

def delete_ride_by_id(db_connection: connection, ride_id: int) -> dict:
    """
    Deletes entry in Ride table with ride_id matching that given, returning a dictionary of the
    deleted entry; returns None if no matches are found.
    """

    with db_connection.cursor() as db_cur:

        query = """
            DELETE FROM Ride
            WHERE ride_id=%s
            RETURNING *
            """
        parameters = (ride_id)
        db_cur.execute(query,parameters)

        ride = db_cur.fetchone()

        if ride:
            return dict(ride)

        return None
    

def get_rider_by_id(db_connection: connection, rider_id: int) -> dict:
    """
    Returns dictionary of the entry in Rider table with rider_id matching that given; returns None
    if no matches are found.
    """

    with db_connection.cursor() as db_cur:

        query = """SELECT * FROM Rider WHERE rider_id=%s"""
        parameters = rider_id
        db_cur.execute(query,parameters)

        rider = db_cur.fetchone()

        if rider:
            return dict(rider)

        return None
    

def get_rider_rides_by_id(db_connection: connection, rider_id: int) -> dict:
    """
    Returns list of dictionaries of the entries in the Ride table with rider_id matching that
    given; returns [] if no matches are found.
    """

    with db_connection.cursor() as db_cur:

        query = """SELECT * FROM Ride WHERE rider_id=%s"""
        parameters = rider_id
        db_cur.execute(query,parameters)

        ride_rows = db_cur.fetchall()

        return [dict(row) for row in ride_rows]


def get_daily_rides(db_connection: connection, date: datetime) -> dict:
    """
    Returns list of dictionaries of the entries in the Ride table with the date of the start_time
    matching that given; returns [] if no matches are found.
    """

    with db_connection.cursor() as db_cur:

        query = """SELECT * FROM Ride WHERE start_time::date = %s"""
        parameters = date
        db_cur.execute(query,parameters)

        ride_rows = db_cur.fetchall()

        return [dict(row) for row in ride_rows]