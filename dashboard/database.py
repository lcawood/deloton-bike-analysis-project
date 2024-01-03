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
    """Fetched the details of the current ride from the database using an SQL Select Query."""
    with db_connection.cursor() as db_cur:

        query = """
        SELECT rider_id, first_name, last_name, height, weight, gender, birthdate
        heart_rate, power, resistance, elapsed_time
        FROM Ride
        JOIN Rider ON Ride.rider_id = Rider.rider_id
        JOIN Reading ON Ride.ride_id = Reading.ride_id
        ORDER BY start_time DESC
        LIMIT 1
        ;
        """

        db_cur.execute(query)

        user_details = db_cur.fetchone()

        return user_details


def get_current_rider_highest_duration(db_cur: extensions.connection.cursor, rider_id: int):
    """Returns the highest historical time_elapsed of the rider with the given rider_id."""
    query = """
    SELECT elapsed_time
    FROM ride
    JOIN Rider ON Ride.rider_id = Rider.rider_id
    JOIN Reading ON Ride.ride_id = Reading.ride_id
    WHERE Ride.rider_id LIKE %s
    ORDER BY elapsed_time DESC
    LIMIT 1
    ;
    """

    db_cur.execute(query, (rider_id, ))

    highest_duration = db_cur.fetchone()

    return highest_duration[0] if highest_duration is not None else None


def get_current_rider_highest_heart_rate(db_cur: extensions.connection.cursor, rider_id: int):
    """Returns the highest historical heart_rate of the rider with the given rider_id."""
    query = """
    SELECT heart_rate
    FROM ride
    JOIN Rider ON Ride.rider_id = Rider.rider_id
    JOIN Reading ON Ride.ride_id = Reading.ride_id
    WHERE Ride.rider_id LIKE %s
    ORDER BY elapsed_time DESC
    LIMIT 1
    ;
    """

    db_cur.execute(query, (rider_id, ))

    highest_heart_rate = db_cur.fetchone()

    return highest_heart_rate[0] if highest_heart_rate is not None else None


def get_current_rider_highest_power(db_cur: extensions.connection.cursor, rider_id: int):
    """Returns the highest historical power of the rider with the given rider_id."""
    query = """
    SELECT power
    FROM ride
    JOIN Rider ON Ride.rider_id = Rider.rider_id
    JOIN Reading ON Ride.ride_id = Reading.ride_id
    WHERE Ride.rider_id LIKE %s
    ORDER BY elapsed_time DESC
    LIMIT 1
    ;
    """

    db_cur.execute(query, (rider_id, ))

    highest_power = db_cur.fetchone()

    return highest_power[0] if highest_power is not None else None


def get_current_rider_highest_resistance(db_cur: extensions.connection.cursor, rider_id: int):
    """Returns the highest historical resistance of the rider with the given rider_id."""
    query = """
    SELECT resistance
    FROM ride
    JOIN Rider ON Ride.rider_id = Rider.rider_id
    JOIN Reading ON Ride.ride_id = Reading.ride_id
    WHERE Ride.rider_id LIKE %s
    ORDER BY elapsed_time DESC
    LIMIT 1
    ;
    """

    db_cur.execute(query, (rider_id, ))

    highest_resistance = db_cur.fetchone()

    return highest_resistance[0] if highest_resistance is not None else None


def get_current_ride_data_highest(db_connection: extensions.connection, rider_details: list) -> int:
    """Fetched the personal highest details of the current ride from the database using an SQL Select Query."""

    with db_connection.cursor() as db_cur:
        rider_id = rider_details[0]
        highest_duration = get_current_rider_highest_duration(db_cur, rider_id)
        highest_heart_rate = get_current_rider_highest_heart_rate(
            db_cur, rider_id)
        highest_power = get_current_rider_highest_power(db_cur, rider_id)
        highest_resistance = get_current_rider_highest_resistance(
            db_cur, rider_id)

        return (hi)
