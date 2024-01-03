"""Utility functions to transform data fetched from SQL for use in visualisations."""


def get_current_rider_name(current_ride: list) -> str:
    """Returns a string containing the rider first and last name."""
    first_name = current_ride[0]
    last_name = current_ride[1]
    rider_name = f"{first_name} {last_name}"
    return rider_name
