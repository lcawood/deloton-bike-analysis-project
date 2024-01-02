"""Module to add the transformed data extracted from the kafka stream to the db."""
from database_functions import get_database_connection

def add_user(user: dict):
    """adds user dictionary as a record in the Rider table in the db."""
    connection = get_database_connection()

    


def add_ride(ride: dict) -> int:
    """adds ride dictionary as a record in the Ride table in the db."""
    pass


def add_reading(reading: dict):
    """adds reading dictionary as a record in the Reading table in the db."""
    pass


def add_bike(bike_serial_number: int) -> int:
    """adds bike serial number as record in Bike table in the db."""