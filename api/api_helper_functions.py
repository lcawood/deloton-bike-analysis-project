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
