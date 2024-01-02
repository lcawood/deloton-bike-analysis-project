"""Testing suite for the validate_heart_rate script."""

import pytest

from validate_heart_rate import calculate_max_heart_rate, calculate_age


@pytest.mark.parametrize('birthdate, age', [
    ('-336700800000', 64),
    ('256700800000', 45),
    ('1111700800000', 18),
    ('571700800000', 35)
])
def test_calculate_age_valid(birthdate: str, age: int):
    assert calculate_age(birthdate) == age


@pytest.mark.parametrize('user_details, threshold', [
    ({"birthdate": '1111700800000', "gender": 'male'}, 202),  # age = 18
    ({"birthdate": '571700800000', "gender": 'male'}, 185),  # age = 35
    ({"birthdate": '1111700800000', "gender": 'female'}, 190),  # age = 18
    ({"birthdate": '571700800000', "gender": 'female'}, 175),  # age = 35
    ({"birthdate": '-336700800000', "gender": 'male'}, 163),  # age = 64
    ({"birthdate": '256700800000', "gender": 'male'}, 176),  # age = 45
    ({"birthdate": '-336700800000', "gender": 'female'}, 150),  # age = 64
    ({"birthdate": '256700800000', "gender": 'female'}, 166)  # age = 45
])
def test_calculate_max_heart_rate_valid(user_details: dict, threshold: int):
    assert calculate_max_heart_rate(user_details) == threshold
