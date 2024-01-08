"""
This script contains functions that:

- Calculate the rider's age
- Create heart rate thresholds for the rider
- Send an email to the rider email.

The thresholds are set based on the maximum heart rate for the rider based on their age and gender.
Several formulas are used to ensure accuracy for different categories of riders.
These are:
- Gulati Formula (women): 206 - (0.88 x age)
- Tanaka Formula (men over age 40): 208 - (0.7 x age)
- Fox formula (men under age 40): 220 - age

The threshold for a minimum heart rate is assumed to be the lower end of an athletes
resting heart rate to also accommodate for high performance riders.
These are:
- Men 18-39: 40
- Men 40-64: 47
- Men 65+: 52
- Women 18-39: 45
- Women 40-64: 52
- Women 65+: 57

"""

from datetime import datetime
from os import environ
import boto3


CHARSET = "UTF-8"


def calculate_age(birthdate: datetime, current_date: datetime = datetime.utcnow()) -> int:
    """
    Returns the age in years for the given date as a datetime object
    in the format YYYY-MM-DD.
    'birthdate' is assumed to be cleaned and always as a datetime type.
    """
    age = current_date.year - birthdate.year - \
        ((current_date.month, current_date.day)
         < (birthdate.month, birthdate.day))

    return age


def calculate_max_heart_rate(rider_details: dict) -> int:
    """
    Returns the maximum heart rate for the given rider based on their age and gender.

    'birthdate' and 'gender' are assumed to be cleaned and
    always as datetime and str types, respectively.

    'other' and 'None' gender heart rates are treated conservatively using the formula
    for females as a safety precaution.
    """
    birthdate = rider_details.get('birthdate')
    age = calculate_age(birthdate)
    gender = rider_details.get('gender')

    if gender in ("female", "other", None):
        return round(206 - (0.88 * age))
    if gender == "male" and age < 40:
        return round(220 - age)

    return round(208 - (0.7 * age))


def calculate_min_heart_rate(rider_details: dict) -> int:
    """
    Returns the minimum heart rate for the given rider based on their age and gender.

    'birthdate' and 'gender' are assumed to be cleaned and
    always as datetime and str types, respectively.

    'other' and 'None' gender heart rates are treated conservatively using the formula
    for females as a safety precaution.
    """
    birthdate = rider_details.get('birthdate')
    age = calculate_age(birthdate)
    gender = rider_details.get('gender')

    if gender in ("female", "other", None):

        if 18 <= age <= 39:
            return 45
        if 40 <= age <= 64:
            return 52
        return 57

    # male
    if 18 <= age <= 39:
        return 40
    if 40 <= age <= 64:
        return 47

    return 52


def send_email(rider_details: dict, extreme_hr_counts: list[int]) -> None:
    """
    Sends an email to the relevant email address using AWS SES,
    assuming the rider email address is already verified.
    """
    ses_client = boto3.client("ses", region_name="eu-west-2")

    rider_email = rider_details.get("email")
    first_name = rider_details.get("first_name")
    last_name = rider_details.get("last_name")

    ses_client.send_email(
        Destination={
            "ToAddresses": [
                rider_email,
            ],
        },
        Message={
            "Body": {
                "Text": {
                    "Charset": CHARSET,
                    "Data": f"""{first_name} {last_name}, your detected heart rate is abnormal!
                    Your heart rate readings are {extreme_hr_counts}.
                    Please rest or seek some help.""",
                }
            },
            "Subject": {
                "Charset": CHARSET,
                "Data": "Deloton Heart Rate Alert",
            },
        },
        Source="trainee.dawid.dawidowski@sigmalabs.co.uk",
    )
