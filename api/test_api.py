"""Unit tests for the api.py file"""

from api import api


def test_api():
    """Tests the api function"""
    assert api() == 0
