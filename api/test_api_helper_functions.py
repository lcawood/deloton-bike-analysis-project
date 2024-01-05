"""Module containing tests for api_helper_functions.py."""

import pytest

from api_helper_functions import format_seconds_as_readable_time


@pytest.mark.parametrize('num_seconds, time_string', [
    (0, '0s'),
    (32, '32s'),
    (60, '1m 0s'),
    (78, '1m 18s'),
    (584, '9m 44s'),
    (2389, '39m 49s'),
    (30034, '8h 20m 34s'),
    (534535, '148h 28m 55s')
])
def test_format_seconds_as_readable_time(num_seconds: int, time_string: str):
    """
    Tests that the function format_seconds_as_readable_time returns time_string for input
    num_seconds.
    """
    assert format_seconds_as_readable_time(num_seconds) == time_string
