"""Testing suite for the validate_heart_rate script."""

import pytest

from validate_heart_rate import calculate_max_heart_rate, calculate_age


@pytest.mark.parametrize('birthdate, age', [
    ('-336700800000', 64)
])
def test_calculate_age_valid(birthdate: str, age: int):
    assert calculate_age(birthdate) == age


@pytest.mark.parametrize('user_details, threshold', [
    ({'age': 18, "gender": 'male'}, 208),
    ({'age': 35, "gender": 'male'}, 185),
    ({'age': 18, "gender": 'female'}, 190),
    ({'age': 35, "gender": 'female'}, 175),
    ({'age': 45, "gender": 'male'}, 177),
    ({'age': 60, "gender": 'male'}, 166),
    ({'age': 45, "gender": 'female'}, 166),
    ({'age': 60, "gender": 'female'}, 153)
])
def test_calculate_max_heart_rate_valid(user_details: dict, threshold: int):
    assert calculate_max_heart_rate(user_details) == threshold
