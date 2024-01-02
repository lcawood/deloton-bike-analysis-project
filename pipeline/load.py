"""Module to add the transformed data extracted from the kafka stream to the db."""
from database_functions import get_database_connection,load_user_into_database

def add_user(user: dict) -> id:
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