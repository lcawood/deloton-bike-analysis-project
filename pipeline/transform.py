"""Module to transform the data received from the Kafka cluster"""


def get_user_from_log_line(log_line: str) -> dict:
    """
    Takes in a kafka log line and returns a dictionary of user data from it (excluding address).
    """
    return {}


def get_ride_data_from_log_line(log_line: str) -> dict:
    """
    Takes in a kafka log line and returns a dictionary of ride data from it (corresponding to
    non-auto-generated attributes in ride table in db).
    """
    return {}


def get_reading_data_from_log_line(reading: dict, log_line: str) -> dict:
    """
    Takes in kafka log line, and transforms and appends reading data contained within it to the
    given reading dictionary.
    """
    return {}


def get_bike_serial_number_from_log_line(log_line: str) -> int:
    """Takes in a kafka log line, and returns bike serial number from it."""
    return 0


def get_address_from_log_line(log_line: str) -> dict:
    """Takes in a kafka log line and returns a dictionary of address data from it."""
    return 0
