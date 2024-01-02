"""Module to add the transformed data extracted from the kafka stream to the db."""

from psycopg2 import errors

from database_functions import get_database_connection,load_user_into_database,load_address_into_database

def add_address(address : dict) -> int:
    """adds a address dictionary as a record in the Address table in the db."""

    connection = get_database_connection()

    try:
        address_id = load_address_into_database(connection,address)
        return address_id
    except errors.UniqueViolation:
        address_id = select_address_from_database(connection,address)
        return address_id


    return address_id

def add_user(user: dict) -> int:
    """adds user dictionary as a record in the Rider table in the db."""
    connection = get_database_connection()

    rider_id = load_user_into_database(connection,user)

    return rider_id


def add_ride(ride: dict) -> int:
    """adds ride dictionary as a record in the Ride table in the db."""
    pass


def add_reading(reading: dict):
    """adds reading dictionary as a record in the Reading table in the db."""
    pass


def add_bike(bike_serial_number: int) -> int:
    """adds bike serial number as record in Bike table in the db."""
    pass