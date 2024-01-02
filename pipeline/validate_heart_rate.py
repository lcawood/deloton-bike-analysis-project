"""Module containing functions to validate a healthy heart rate."""

from datetime import datetime

def calculate_age(birthdate: datetime) -> int:
    """
    Returns the age in years for the given date as a datetime object in the format YYYY-MM-DD.
    """
    return 0


def calculate_max_heart_rate(user_details: dict) -> int:
    """Returns the maximum heart rate for the given user based on their age and gender."""
    return 0


def calculate_min_heart_rate(user_details: dict) -> int:
    """Returns the minimum heart rate for the given user based on their age and gender."""
    return 0


def send_email(user_details: dict, extreme_hr_counts: list[int]) -> None:
    """Sends an email to the relevant email address using AWS SES."""
    pass
