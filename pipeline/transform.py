"""Module to transform the data received from the Kafka cluster"""


def get_user_from_log_line(log_line: str) -> dict:
    """Takes in a kafka log line and returns a dictionary of user data from it (excluding address)."""
    pass


def get_ride_data_from_log_line(log_line: str) -> dict:
    """
    Takes in a kafka log line and returns a dictionary of ride data from it (corresponding to
    non-auto-generated attributes in ride table in db).
    """
    pass


def get_reading_data_from_log_line(reading: dict, log_line: str) -> dict:
    """
    Takes in kafka log line, and transforms and appends reading data contained within it to the given reading
    dictionary.
    """
    pass


def get_address_from_log_line(log_line: str) -> dict:
    """Takes in a kafka log line and returns a dictionary of address data from it."""
    pass