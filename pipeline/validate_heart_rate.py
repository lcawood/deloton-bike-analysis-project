"""
This script contains functions that:

- Create heart rate thresholds for the user
- Validate heart rate doesn't exceed threshold
- Send an email to the user email.

The thresholds are set based on the maximum heart rate for the user based on their age and gender.
Several formulas are used to ensure accuracy for different categories of users.
These are:
- Gulati Formula (women): 206 - (0.88 x age)
- Tanaka Formula (men over age 40): 208 - (0.7 x age)
- Fox formula (men under age 40): 220 - age
"""

from datetime import datetime


def calculate_age(birthdate: str) -> int:
    """
    Returns the age in years for the given date in
    milliseconds since the Unix epoch (January 1, 1970, 00:00:00 UTC).
    """
    birthdate_in_seconds = int(birthdate)/1000
    birth_date = datetime.utcfromtimestamp(birthdate_in_seconds)
    current_date = datetime.utcnow()

    age = current_date.year - birth_date.year - \
        ((current_date.month, current_date.day)
         < (birth_date.month, birth_date.day))

    return age


def calculate_max_heart_rate(user_details: dict) -> int:
    """Returns the maximum heart rate for the given user based on their age and gender."""
    pass
