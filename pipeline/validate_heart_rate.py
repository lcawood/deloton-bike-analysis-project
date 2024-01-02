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

The threshold for a minimum heart rate is assumed to be the lower end of an athletes
resting heart rate to also accommodate for high performance users.
These are:
- Men 18-39: 40
- Men 40-64: 47
- Men 65+: 52
- Women 18-39: 45
- Women 40-64: 52
- Women 65+: 57

"""

from datetime import datetime

# TODO assume birthdate is in date format.


def calculate_age(birthdate: str) -> int:
    """
    Returns the age in years for the given date in
    milliseconds since the Unix epoch (January 1, 1970, 00:00:00 UTC).
    """
    birthdate_in_seconds = int(birthdate)/1000
    print(birthdate_in_seconds)
    birth_date = datetime.utcfromtimestamp(birthdate_in_seconds)
    print(birth_date)
    current_date = datetime.utcnow()
    print(current_date)

    age = current_date.year - birth_date.year - \
        ((current_date.month, current_date.day)
         < (birth_date.month, birth_date.day))

    return age


def calculate_max_heart_rate(user_details: dict) -> int:
    """Returns the maximum heart rate for the given user based on their age and gender."""
    birthdate = user_details.get('birthdate')
    age = calculate_age(birthdate)
    gender = user_details.get('gender')

    if gender == "female":
        return round(206 - (0.88 * age))
    elif gender == "male" and age < 40:
        return round(220 - age)
    elif gender == "male" and age >= 40:
        return round(208 - (0.7 * age))
    return 0


def calculate_min_heart_rate(user_details: dict) -> int:
    """Returns the minimum heart rate for the given user based on their age and gender."""
    birthdate = user_details.get('birthdate')
    age = calculate_age(birthdate)
    gender = user_details.get('gender')

    if gender == "female":

        if 18 <= age <= 39:
            return 45
        elif 40 <= age <= 64:
            return 52
        elif age >= 65:
            return 57

    elif gender == "male":

        if 18 <= age <= 39:
            return 40
        elif 40 <= age <= 64:
            return 47
        elif age >= 65:
            return 52

    return 0


def send_email() -> None:
    """Sends an email to the relevant email address using AWS SES."""
    pass
