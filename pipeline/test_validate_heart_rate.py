"""
Testing suite for the validate_heart_rate script.

The global constant ages are valid as of January 2024.
"""

from datetime import datetime
from unittest.mock import patch

import pytest

from validate_heart_rate import (calculate_max_heart_rate, calculate_min_heart_rate,
                                 calculate_age, send_email)

BIRTHDATE_AGE_64 = datetime.strptime('1960-01-01', "%Y-%m-%d")
BIRTHDATE_AGE_45 = datetime.strptime('1979-01-01', "%Y-%m-%d")
BIRTHDATE_AGE_18 = datetime.strptime('2006-01-01', "%Y-%m-%d")
BIRTHDATE_AGE_35 = datetime.strptime('1989-01-01', "%Y-%m-%d")
BIRTHDATE_AGE_65 = datetime.strptime('1959-01-01', "%Y-%m-%d")
TEST_CURRENT_DATE = datetime.strptime('2024-01-03', "%Y-%m-%d")


@pytest.mark.parametrize('birthdate, age', [
    (BIRTHDATE_AGE_64, 64),
    (BIRTHDATE_AGE_45, 45),
    (BIRTHDATE_AGE_18, 18),
    (BIRTHDATE_AGE_35, 35),
    (BIRTHDATE_AGE_65, 65)
])
def test_calculate_age_valid(birthdate: datetime, age: int):
    """Test that the expected ages are returned for the given datetime objects."""
    assert calculate_age(birthdate, TEST_CURRENT_DATE) == age


@pytest.mark.parametrize('user_details, threshold', [
    ({"birthdate": BIRTHDATE_AGE_18, "gender": 'male'}, 202),  # age = 18
    ({"birthdate": BIRTHDATE_AGE_35, "gender": 'male'}, 185),  # age = 35
    ({"birthdate": BIRTHDATE_AGE_18, "gender": 'female'}, 190),  # age = 18
    ({"birthdate": BIRTHDATE_AGE_35, "gender": 'female'}, 175),  # age = 35
    ({"birthdate": BIRTHDATE_AGE_64, "gender": 'male'}, 163),  # age = 64
    ({"birthdate": BIRTHDATE_AGE_45, "gender": 'male'}, 176),  # age = 45
    ({"birthdate": BIRTHDATE_AGE_64, "gender": 'female'}, 150),  # age = 64
    ({"birthdate": BIRTHDATE_AGE_45, "gender": 'other'}, 166),  # age = 45
    ({"birthdate": BIRTHDATE_AGE_18, "gender": 'other'}, 190),  # age = 18
    ({"birthdate": BIRTHDATE_AGE_35, "gender": 'other'}, 175),  # age = 35
    ({"birthdate": BIRTHDATE_AGE_64, "gender": 'other'}, 150),  # age = 64
    ({"birthdate": BIRTHDATE_AGE_45, "gender": None}, 166),  # age = 45
    ({"birthdate": BIRTHDATE_AGE_45, "gender": None}, 166),  # age = 45
    ({"birthdate": BIRTHDATE_AGE_18, "gender": None}, 190),  # age = 18
    ({"birthdate": BIRTHDATE_AGE_35, "gender": None}, 175),  # age = 35
    ({"birthdate": BIRTHDATE_AGE_64, "gender": None}, 150),  # age = 64
    ({"birthdate": BIRTHDATE_AGE_45, "gender": None}, 166)  # age = 45
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
    ({"birthdate": BIRTHDATE_AGE_65, "gender": 'female'}, 57),  # age = 65
    ({"birthdate": BIRTHDATE_AGE_18, "gender": 'other'}, 45),  # age = 18
    ({"birthdate": BIRTHDATE_AGE_35, "gender": 'other'}, 45),  # age = 35
    ({"birthdate": BIRTHDATE_AGE_64, "gender": 'other'}, 52),  # age = 64
    ({"birthdate": BIRTHDATE_AGE_45, "gender": 'other'}, 52),  # age = 45
    ({"birthdate": BIRTHDATE_AGE_65, "gender": 'other'}, 57),  # age = 65
    ({"birthdate": BIRTHDATE_AGE_18, "gender": None}, 45),  # age = 18
    ({"birthdate": BIRTHDATE_AGE_35, "gender": None}, 45),  # age = 35
    ({"birthdate": BIRTHDATE_AGE_64, "gender": None}, 52),  # age = 64
    ({"birthdate": BIRTHDATE_AGE_45, "gender": None}, 52),  # age = 45
    ({"birthdate": BIRTHDATE_AGE_65, "gender": None}, 57)  # age = 65
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
