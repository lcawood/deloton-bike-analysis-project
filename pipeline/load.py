"""Module to add the transformed data extracted from the kafka stream to the db."""

from psycopg2 import errors

from database_functions import get_database_connection,load_user_into_database,load_address_into_database,select_address_from_database,load_ride_into_database,select_ride_from_database,load_reading_into_database,select_reading_from_database,load_bike_into_database,select_bike_from_database

def add_address(address : dict) -> int:
    """adds a address dictionary as a record in the Address table in the db."""

    connection = get_database_connection()

    try:
        address_id = load_address_into_database(connection,address)
        return address_id
    except errors.UniqueViolation:
        connection.commit()
        address_id = select_address_from_database(connection,address)
        return address_id


def add_user(user: dict) -> int:
    """adds user dictionary as a record in the Rider table in the db."""
    connection = get_database_connection()

    try:
        rider_id = load_user_into_database(connection,user)
        return rider_id
    except errors.UniqueViolation:
        return user["user_id"]


def add_ride(ride: dict) -> int:
    """adds ride dictionary as a record in the Ride table in the db."""
    connection = get_database_connection()

    try:
        ride_id = load_ride_into_database(connection,ride)
        return ride_id
    except errors.UniqueViolation:
        connection.commit()
        ride_id = select_ride_from_database(connection,ride)
        return ride_id


def add_reading(reading: dict):
    """adds reading dictionary as a record in the Reading table in the db."""
    connection = get_database_connection()

    try:
        reading_id = load_reading_into_database(connection,reading)
        return reading_id
    except errors.UniqueViolation:
        connection.commit()
        reading_id = select_reading_from_database(connection,reading)
        return reading_id


def add_bike(bike_serial_number: int) -> int:
    """adds bike serial number as record in Bike table in the db."""
    connection = get_database_connection()

    try:
        bike_id = load_bike_into_database(connection,bike_serial_number)
        return bike_id
    except errors.UniqueViolation:
        connection.commit()
        bike_id = select_bike_from_database(connection,bike_serial_number)
        return bike_id