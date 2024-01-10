"""Contains helper functions for the api."""

def format_seconds_as_readable_time(seconds: int) -> str:
    """
    Takes in an integer number of seconds, and returns a human readable string of the time in
    hours, minutes and seconds.
    """
    hours = seconds // (60 ** 2)
    seconds -= hours * (60 ** 2)
    minutes = seconds // 60
    seconds -= minutes * 60

    time_string = f"{seconds}s"

    if minutes:
        time_string = f'{minutes}m ' + time_string

    if hours:
        time_string = f'{hours}h ' + time_string

    return time_string


def is_positive_integer(num: int) -> bool:
    """Function to verify a given (id) number is a positive integer."""
    return isinstance(num, int) and (num > 0)


def is_string_boolean(string: str) -> bool:
    """
    Function to return whether the input string is a representation of a boolean either 'True' or
    'False'.
    """
    return string in ['False', 'True']
