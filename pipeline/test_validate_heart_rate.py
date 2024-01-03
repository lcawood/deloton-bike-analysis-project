"""
Testing suite for the validate_heart_rate script.

The global constant ages are valid as of January 2024.
"""

from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from validate_heart_rate import (calculate_max_heart_rate, calculate_min_heart_rate,
                                 calculate_age, send_email)


# Function to calculate birthdate based on age
def calculate_birthdate(age):
    """Calculates and returns birthdate based on age for use in unit tests."""
    current_date = datetime.now()
    birthdate = current_date - timedelta(days=(365.25 * age))
    return birthdate


BIRTHDATE_AGE_64 = calculate_birthdate(64)
BIRTHDATE_AGE_45 = calculate_birthdate(45)
BIRTHDATE_AGE_18 = calculate_birthdate(18)
BIRTHDATE_AGE_35 = calculate_birthdate(35)
BIRTHDATE_AGE_65 = calculate_birthdate(65)


@pytest.mark.parametrize('birthdate, age', [
    (BIRTHDATE_AGE_64, 64),
    (BIRTHDATE_AGE_45, 45),
    (BIRTHDATE_AGE_18, 18),
    (BIRTHDATE_AGE_35, 35),
    (BIRTHDATE_AGE_65, 65)
])
def test_calculate_age_valid(birthdate: str, age: int):
    """Test that the expected ages are returned for the given datetime objects."""
    assert calculate_age(birthdate) == age


@pytest.mark.parametrize('user_details, threshold', [
    ({"birthdate": BIRTHDATE_AGE_18, "gender": 'male'}, 202),  # age = 18
    ({"birthdate": BIRTHDATE_AGE_35, "gender": 'male'}, 185),  # age = 35
    ({"birthdate": BIRTHDATE_AGE_18, "gender": 'female'}, 190),  # age = 18
    ({"birthdate": BIRTHDATE_AGE_35, "gender": 'female'}, 175),  # age = 35
    ({"birthdate": BIRTHDATE_AGE_64, "gender": 'male'}, 163),  # age = 64
    ({"birthdate": BIRTHDATE_AGE_45, "gender": 'male'}, 176),  # age = 45
    ({"birthdate": BIRTHDATE_AGE_64, "gender": 'female'}, 150),  # age = 64
    ({"birthdate": BIRTHDATE_AGE_45, "gender": 'female'}, 166)  # age = 45
])
def test_calculate_max_heart_rate_valid(user_details: dict, threshold: int):
    """Test that the expected maximum heart rates are returned for the given users."""
    assert calculate_max_heart_rate(user_details) == threshold


@pytest.mark.parametrize('user_details, threshold', [
    ({"birthdate": BIRTHDATE_AGE_18, "gender": 'male'}, 40),  # age = 18
    ({"birthdate": BIRTHDATE_AGE_35, "gender": 'male'}, 40),  # age = 35
    ({"birthdate": BIRTHDATE_AGE_18, "gender": 'female'}, 45),  # age = 18
    ({"birthdate": BIRTHDATE_AGE_35, "gender": 'female'}, 45),  # age = 35
    ({"birthdate": BIRTHDATE_AGE_64, "gender": 'male'}, 47),  # age = 64
    ({"birthdate": BIRTHDATE_AGE_45, "gender": 'male'}, 47),  # age = 45
    ({"birthdate": BIRTHDATE_AGE_64, "gender": 'female'}, 52),  # age = 64
    ({"birthdate": BIRTHDATE_AGE_45, "gender": 'female'}, 52),  # age = 45
    ({"birthdate": BIRTHDATE_AGE_65, "gender": 'male'}, 52),  # age = 65
    ({"birthdate": BIRTHDATE_AGE_65, "gender": 'female'}, 57)  # age = 65
])
def test_calculate_min_heart_rate_valid(user_details: dict, threshold: int):
    """Test that the expected minimum heart rates are returned for the given users."""
    assert calculate_min_heart_rate(user_details) == threshold


@patch('boto3.client')
def test_ses_send_email(mock_boto_client):
    """send_email() should call .send_email once to send an SES email."""

    mock_boto_client.return_value = mock_boto_client
    mock_boto_client.get_parameter.return_value = {}

    fake_user_details = {
        "first_name": "John",
        "last_name": "Doe",
        "email": "fake_email@hotmail.com"
    }

    fake_hr_counts = [200, 200, 200]

    send_email(fake_user_details, fake_hr_counts)

    mock_boto_client.send_email.assert_called_once()
