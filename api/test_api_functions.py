"""
Module containing tests for the functions in module api_functions;

Each test either:
 - feeds in valid data and checks the appropriate functions from database_functions are called,
    and that the appropriate results from these function calls are returned, along with a 200
    status code;
 - feeds in invalid data, and checks that the database_functions are not called, and that an
    appropriate error dictionary and status code are returned.
"""

from datetime import datetime

import pytest
from unittest.mock import patch
import psycopg2.errors

from api_functions import get_ride, get_rider


EXAMPLE_ADDRESS = {"first_line" : "63 Studio","second_line" : "Nursery Avenue",
                   "city" : "London", "postcode" : "LA1 34A"}


EXAMPLE_READING = {"ride_id" : 1, "heart_rate" : 76, "power" : 12.6423, "rpm" : 20,
                   "resistance" : 50, "elapsed_time" : 120}

EXAMPLE_BIKE_SERIAL = "SD2e4219u"


class TestGetRide():
    """Unit tests testing the function get_ride for various inputs"""

    @pytest.fixture(scope='function', autouse=True)
    def mock_db_get_ride(self):
        """Patch over db function used in all tests in class."""
        with patch('database_functions.get_ride_by_id') as mocked_function:
            yield mocked_function


    @pytest.mark.parametrize('ride_id', [
        (False),
        ('four'),
        (8.2),
        (-1),
    ])
    def test_invalid_ride_id_type(self, mock_db_get_ride, ride_id):
        """Tests function get_ride correctly handles a ride_id with invalid type."""
        assert get_ride(None, ride_id) == ({'error': 'Invalid url; ride_id must be a positive integer.'}, 400)
        mock_db_get_ride.assert_not_called()


    def test_invalid_ride_id_value(self, mock_db_get_ride):
        """
        Tests function get_ride correctly handles a valid type ride_id matching no ride in db.
        """
        mock_db_get_ride.return_value = None
        assert get_ride(None, 4) == ({'error': f'Ride with id 4 could not be found.'}, 404)
        mock_db_get_ride.assert_called_with(None, 4)


    @pytest.mark.parametrize('error', [
        (psycopg2.errors.AmbiguousAlias),
        (psycopg2.errors.CannotCoerce),
        (psycopg2.errors.ConnectionFailure),
        (psycopg2.errors.FloatingPointException),
    ])
    def test_invalid_db_error(self, mock_db_get_ride, error):
        """
        Tests that the function get_ride correctly handles a variety of errors in fetching the data
        from the db (the errors chosen are not thought to be typical, but hopefully span enough
        psycopg2 errors to prove the function's error handling covers the range of them).
        """
        mock_db_get_ride.side_effect = error('Something has gone horribly wrong!')
        assert get_ride(None, 4) == ({'error': 'Something has gone horribly wrong!'}, 500)
        mock_db_get_ride.assert_called_once_with(None, 4)


    def test_valid(self, mock_db_get_ride):
        """
        Tests that the function get_ride correctly handles a ride_id of valid type and value, with
        a valid return from the db function get_ride_by_id.
        """
        example_ride = {
            "user_id" : 1234,
            "bike_id": 1,
            "start_time" : datetime.strptime("07/03/2021 16:21:12", "%d/%m/%Y %H:%M:%S")
            }
        mock_db_get_ride.return_value = example_ride

        assert get_ride(None, 1) == (example_ride, 200)
        mock_db_get_ride.assert_called_once_with(None, 1)



class TestGetRider():
    """Unit tests testing the function get_rider for various inputs"""

    @pytest.fixture(scope='function')
    def mock_db_get_rider(self):
        """Patch over db function used in all tests in class."""
        with patch('database_functions.get_rider_by_id') as mocked_function:
            yield mocked_function


    @pytest.mark.parametrize('rider_id', [
        (False),
        ('four'),
        (8.2),
        (-1),
    ])
    def test_invalid_rider_id_type(self, mock_db_get_rider, rider_id):
        """Tests function get_rider correctly handles a rider_id with invalid type."""
        assert get_rider(None, rider_id) == ({'error': 'Invalid url; rider_id must be a positive integer.'}, 400)
        mock_db_get_rider.assert_not_called()


    def test_invalid_rider_id_value(self, mock_db_get_rider):
        """
        Tests function get_rider correctly handles a valid type rider_id matching no rider in db.
        """
        mock_db_get_rider.return_value = None
        assert get_rider(None, 4) == ({'error': f'Rider with id 4 could not be found.'}, 404)
        mock_db_get_rider.assert_called_with(None, 4)


    @pytest.mark.parametrize('error', [
        (psycopg2.errors.AmbiguousAlias),
        (psycopg2.errors.CannotCoerce),
        (psycopg2.errors.ConnectionFailure),
        (psycopg2.errors.FloatingPointException),
    ])
    def test_invalid_db_error(self, mock_db_get_rider, error):
        """
        Tests that the function get_rider correctly handles a variety of errors in fetching the data
        from the db (the errors chosen are not thought to be typical, but hopefully span enough
        psycopg2 errors to prove the function's error handling covers the range of them).
        """
        mock_db_get_rider.side_effect = error('Something has gone horribly wrong!')
        assert get_rider(None, 4) == ({'error': 'Something has gone horribly wrong!'}, 500)
        mock_db_get_rider.assert_called_once_with(None, 4)


    def test_valid(self, mock_db_get_rider):
        """
        Tests that the function get_rider correctly handles a rider_id of valid type and value, with
        a valid return from the db function get_rider_by_id.
        """
        example_rider = {
            'user_id' : 1,
            'address_id' : 6,
            'first_name': "John",
            'last_name': "Doe",
            'birthdate': "2002-04-13",
            'height' : 152,
            'weight' : 280,
            'email' : "charlie@gmail.com",
            'gender' : "male",
            'account_created' : "2021-09-27"
            }
        mock_db_get_rider.return_value = example_rider

        assert get_rider(None, 1) == (example_rider, 200)
        mock_db_get_rider.assert_called_once_with(None, 1)



