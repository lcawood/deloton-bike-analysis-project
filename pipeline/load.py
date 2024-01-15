"""Module to add the transformed data extracted from the kafka stream to the database."""

# pylint: disable=C0301
# pylint: disable=E1101

import pandas as pd
from psycopg2 import errors
from psycopg2.extensions import connection

import database_functions


def add_address(db_connection: connection, address : dict) -> int:
    """Adds a address dictionary as a record in the Address table in the db."""

    try:
        address_id = database_functions.load_address_into_database(db_connection, address)
        db_connection.commit()
        return address_id

    except errors.UniqueViolation:
        db_connection.rollback()
        address_id = database_functions.select_address_from_database(db_connection,address)
        return address_id


def add_rider(db_connection: connection, rider: dict) -> int:
    """Adds rider dictionary as a record in the Rider table in the db."""

    try:
        rider_id = database_functions.load_rider_into_database(db_connection, rider)
        db_connection.commit()
        return rider_id

    except errors.UniqueViolation:
        db_connection.rollback()
        return rider["rider_id"]


def add_ride(db_connection: connection, ride: dict) -> int:
    """Adds ride dictionary as a record in the Ride table in the db."""

    try:
        ride_id = database_functions.load_ride_into_database(db_connection, ride)
        db_connection.commit()
        return ride_id

    except errors.UniqueViolation:
        db_connection.rollback()
        ride_id = database_functions.select_ride_from_database(db_connection, ride)
        return ride_id


def add_reading(db_connection: connection, reading: dict) -> int:
    """Adds reading dictionary as a record in the Reading table in the db."""

    try:
        reading_id = database_functions.load_reading_into_database(db_connection, reading)
        db_connection.commit()
        return reading_id

    except (errors.UniqueViolation, errors.NotNullViolation):
        db_connection.rollback()
        reading_id = database_functions.select_reading_from_database(db_connection, reading)
        return reading_id


def add_bike(db_connection: connection, bike_serial_number: int) -> int:
    """
    Adds bike as record in Bike table in the db. 
    Uses the bike serial to add or select the bike.
    """

    try:
        bike_id = database_functions.load_bike_into_database(db_connection, bike_serial_number)
        db_connection.commit()
        return bike_id

    except errors.UniqueViolation:
        db_connection.rollback()
        bike_id = database_functions.select_bike_from_database(db_connection, bike_serial_number)
        return bike_id


def add_readings_from_csv(db_connection: connection, readings_file: str) -> bool:
    """Adds Reading entries directly into the db from a csv."""

    try:
        database_functions.load_readings_into_database_from_csv(db_connection, readings_file)
        db_connection.commit()

    except (errors.UniqueViolation, errors.NotNullViolation):
        db_connection.rollback()
        # We don't know which record caused the unique violation, so have to individually insert all.
        readings = pd.read_csv(readings_file).to_dict("records")
        for reading in readings:
            add_reading(db_connection, reading)
