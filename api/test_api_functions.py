"""
Module containing tests for the functions in module api_functions.py;

Each test either:
 - feeds in valid data and checks the appropriate functions from database_functions are called,
    and that the appropriate results from these function calls are returned, along with a 200
    status code;
 - feeds in invalid data, and checks that the database_functions are not called, and that an
    appropriate error dictionary and status code are returned.
"""

# pylint: disable=E1101
# ^^^ pylint wasn't recognising psycopg2 errors correctly.

from datetime import datetime
from unittest.mock import patch

import psycopg2.errors
import pytest

from api_functions import get_ride, get_rider, get_rider_rides, get_daily_rides, delete_ride



class TestGetRide():
    """Unit tests testing the function get_ride for various inputs."""

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
        assert get_ride(None, ride_id) == (
            {'error': {
                'code': 400,
                'type': 'bad request'
            },
            'message': 'Invalid url; ride_id must be a positive integer.'
            }, 400)
        mock_db_get_ride.assert_not_called()


    def test_invalid_ride_id_value(self, mock_db_get_ride):
        """
        Tests function get_ride correctly handles a valid type ride_id matching no ride in db.
        """
        mock_db_get_ride.return_value = None
        assert get_ride(None, 4) == (
            {'error': {
                'code': 404,
                'type': 'not found'
            },
            'message': 'Ride with id 4 could not be found.'}, 404)
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
        assert get_ride(None, 4) == (
            {'error': {
                'code': 500,
                'type': 'server error'
            },
            'message': 'Oops! There has been a problem on our end; \
please be patient while we reset our database connection (if this problem persists, please \
contact IT support).'}, 500)
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
    """Unit tests testing the function get_rider for various inputs."""

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
        assert get_rider(None, rider_id) == (
            {'error': {
                'code': 400,
                'type': 'bad request'
            },
            'message': 'Invalid url; rider_id must be a positive integer.'}, 400)
        mock_db_get_rider.assert_not_called()


    def test_invalid_rider_id_value(self, mock_db_get_rider):
        """
        Tests function get_rider correctly handles a valid type rider_id matching no rider in db.
        """
        mock_db_get_rider.return_value = None
        assert get_rider(None, 4) == (
            {'error': {
                'code': 404,
                'type': 'not found'
            },
            'message': 'Rider with id 4 could not be found.'}, 404)
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
        assert get_rider(None, 4) == (
            {'error': {
                'code': 500,
                'type': 'server error'
            },
            'message': 'Oops! There has been a problem on our end; \
please be patient while we reset our database connection (if this problem persists, please \
contact IT support).'}, 500)
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



class TestGetRiderRides():
    """Unit tests testing the function get_rider_rides for various inputs."""

    @pytest.fixture(scope='function')
    def mock_db_get_rider_rides(self):
        """Patch over db function used in all tests in class."""
        with patch('database_functions.get_rider_rides_by_id') as mocked_function:
            yield mocked_function


    @pytest.mark.parametrize('rider_id', [
        (False),
        ('four'),
        (8.2),
        (-1),
    ])
    def test_invalid_rider_id_type(self, mock_db_get_rider_rides, rider_id):
        """Tests function get_rider_rides correctly handles a rider_id with invalid type."""
        assert get_rider_rides(None, rider_id) == (
            {'error': {
                'code': 400,
                'type': 'bad request'
            },
            'message': 'Invalid url; rider_id must be a positive integer.'}, 400)
        mock_db_get_rider_rides.assert_not_called()


    def test_invalid_rider_id_value(self, mock_db_get_rider_rides):
        """
        Tests function get_rider_rides correctly handles a valid type rider_id matching no rider in
        db.
        """
        mock_db_get_rider_rides.return_value = []
        assert get_rider_rides(None, 4) == (
            {'error': {
                'code': 404,
                'type': 'not found'
            },
            'message': 'Unable to locate any rides belonging to a rider with id 4.'}, 404)
        mock_db_get_rider_rides.assert_called_with(None, 4)


    @pytest.mark.parametrize('error', [
        (psycopg2.errors.AmbiguousAlias),
        (psycopg2.errors.CannotCoerce),
        (psycopg2.errors.ConnectionFailure),
        (psycopg2.errors.FloatingPointException),
    ])
    def test_invalid_db_error(self, mock_db_get_rider_rides, error):
        """
        Tests that the function get_rider_rides correctly handles a variety of errors in fetching
        the data from the db (the errors chosen are not thought to be typical, but hopefully span
        enough psycopg2 errors to prove the function's error handling covers the range of them).
        """
        mock_db_get_rider_rides.side_effect = error('Something has gone horribly wrong!')
        assert get_rider_rides(None, 4) == (
            {'error': {
                'code': 500,
                'type': 'server error'
            },
            'message': 'Oops! There has been a problem on our end; \
please be patient while we reset our database connection (if this problem persists, please \
contact IT support).'}, 500)
        mock_db_get_rider_rides.assert_called_once_with(None, 4)


    def test_valid(self, mock_db_get_rider_rides):
        """
        Tests that the function get_rider_rides correctly handles a rider_id of valid type and
        value, with a valid return from the db function get_rider_rides_by_id.
        """
        example_rides = [
            {
                "user_id" : 12,
                "bike_id": 1,
                "start_time" : datetime.strptime("06/03/2021 16:21:12", "%d/%m/%Y %H:%M:%S")
            },
            {
                "user_id" : 12,
                "bike_id": 1,
                "start_time" : datetime.strptime("07/03/2021 18:21:12", "%d/%m/%Y %H:%M:%S")
            },
            {
                "user_id" : 12,
                "bike_id": 2,
                "start_time" : datetime.strptime("07/03/2021 16:53:12", "%d/%m/%Y %H:%M:%S")
            },
            {
                "user_id" : 12,
                "bike_id": 1,
                "start_time" : datetime.strptime("09/03/2021 16:21:12", "%d/%m/%Y %H:%M:%S")
            }
        ]
        mock_db_get_rider_rides.return_value = example_rides

        assert get_rider_rides(None, 1) == (example_rides, 200)
        mock_db_get_rider_rides.assert_called_once_with(None, 1)



class TestGetDailyRides():
    """Unit tests testing the function get_daily_rides for various inputs."""

    @pytest.fixture(scope='function')
    def mock_db_get_daily_rides(self):
        """Patch over db function used in all tests in class."""
        with patch('database_functions.get_daily_rides') as mocked_function:
            yield mocked_function


    @pytest.mark.parametrize('date', [
        (False),
        (12351235),
        ("4th March 1998"),
        ("03-13-2021"),
        ("32-03-1987"),
        ("2021-03-08"),
        ("21/03/1987")
    ])
    def test_invalid_date_type(self, mock_db_get_daily_rides, date):
        """
        Tests function get_daily_rides correctly handles a date with invalid type or structure.
        """
        assert get_daily_rides(None, date) == (
            {'error': {
                'code': 400,
                'type': 'bad request'
            },
            'message': 'Invalid url; date must be a datetime string matching the format \
dd-mm-yyyy.'},
            400
            )
        mock_db_get_daily_rides.assert_not_called()


    def test_invalid_date_value(self, mock_db_get_daily_rides):
        """
        Tests function get_daily_rides correctly handles a valid format and type date matching no
        start_date in db.
        """
        test_date = "09-03-1991"
        mock_db_get_daily_rides.return_value = []
        assert get_daily_rides(None, test_date) == (
            {'error': {
                'code': 404,
                'type': 'not found'
            },
            'message': f'Unable to locate any rides starting on {test_date}.'}, 404)
        mock_db_get_daily_rides.assert_called_with(
            None, datetime.strptime(test_date, "%d-%m-%Y").date())


    @pytest.mark.parametrize('error', [
        (psycopg2.errors.AmbiguousAlias),
        (psycopg2.errors.CannotCoerce),
        (psycopg2.errors.ConnectionFailure),
        (psycopg2.errors.FloatingPointException),
    ])
    def test_invalid_db_error(self, mock_db_get_daily_rides, error):
        """
        Tests that the function get_daily_rides correctly handles a variety of errors in fetching
        the data from the db (the errors chosen are not thought to be typical, but hopefully span
        enough psycopg2 errors to prove the function's error handling covers the range of them).
        """
        test_date = "09-03-1991"
        mock_db_get_daily_rides.side_effect = error('Something has gone horribly wrong!')
        assert get_daily_rides(None, test_date) == (
            {'error': {
                'code': 500,
                'type': 'server error'
            },
            'message': 'Oops! There has been a problem on our end; \
please be patient while we reset our database connection (if this problem persists, please \
contact IT support).'}, 500)
        mock_db_get_daily_rides.assert_called_once_with(
            None, datetime.strptime(test_date, "%d-%m-%Y").date())


    def test_valid(self, mock_db_get_daily_rides):
        """
        Tests that the function get_daily_rides correctly handles a date of valid type, format, and
        value, with a valid return from the db function get_daily_rides_by_id.
        """
        example_rides = [
            {
                "user_id" : 12,
                "bike_id": 1,
                "start_time" : datetime.strptime("06/03/2021 16:21:12", "%d/%m/%Y %H:%M:%S")
            },
            {
                "user_id" : 12,
                "bike_id": 1,
                "start_time" : datetime.strptime("06/03/2021 18:21:12", "%d/%m/%Y %H:%M:%S")
            },
            {
                "user_id" : 12,
                "bike_id": 2,
                "start_time" : datetime.strptime("06/03/2021 09:53:12", "%d/%m/%Y %H:%M:%S")
            },
            {
                "user_id" : 12,
                "bike_id": 1,
                "start_time" : datetime.strptime("06/03/2021 21:21:12", "%d/%m/%Y %H:%M:%S")
            }
        ]
        test_date = "09-03-1991"
        mock_db_get_daily_rides.return_value = example_rides

        assert get_daily_rides(None, test_date) == (example_rides, 200)
        mock_db_get_daily_rides.assert_called_once_with(
            None, datetime.strptime(test_date, "%d-%m-%Y").date())



class TestDeleteRide():
    """Unit tests testing the function delete_ride for various inputs."""

    @pytest.fixture(scope='function', autouse=True)
    def mock_db_delete_ride(self):
        """Patch over db function used in all tests in class."""
        with patch('database_functions.delete_ride_by_id') as mocked_function:
            yield mocked_function


    @pytest.mark.parametrize('ride_id', [
        (False),
        ('four'),
        (8.2),
        (-1),
    ])
    def test_invalid_ride_id_type(self, mock_db_delete_ride, ride_id):
        """Tests function delete_ride correctly handles a ride_id with invalid type."""
        assert delete_ride(None, ride_id) == (
            {'error': {
                'code': 400,
                'type': 'bad request'
            },
            'message': 'Invalid url; ride_id must be a positive integer.'}, 400)
        mock_db_delete_ride.assert_not_called()


    def test_invalid_ride_id_value(self, mock_db_delete_ride):
        """
        Tests function delete_ride correctly handles a valid type ride_id matching no ride in db.
        """
        mock_db_delete_ride.return_value = None
        assert delete_ride(None, 4) == (
            {'error': {
                'code': 404,
                'type': 'not found'
            },
            'message': 'Ride with id 4 could not be found.'}, 404)
        mock_db_delete_ride.assert_called_with(None, 4)


    @pytest.mark.parametrize('error', [
        (psycopg2.errors.AmbiguousAlias),
        (psycopg2.errors.CannotCoerce),
        (psycopg2.errors.ConnectionFailure),
        (psycopg2.errors.FloatingPointException),
    ])
    def test_invalid_db_error(self, mock_db_delete_ride, error):
        """
        Tests that the function delete_ride correctly handles a variety of errors in fetching the
        data from the db (the errors chosen are not thought to be typical, but hopefully span
        enough psycopg2 errors to prove the function's error handling covers the range of them).
        """
        mock_db_delete_ride.side_effect = error('Something has gone horribly wrong!')
        assert delete_ride(None, 4) == (
            {'error': {
                'code': 500,
                'type': 'server error'
            },
            'message': 'Oops! There has been a problem on our end; \
please be patient while we reset our database connection (if this problem persists, please \
contact IT support).'},500)
        mock_db_delete_ride.assert_called_once_with(None, 4)


    def test_valid(self, mock_db_delete_ride):
        """
        Tests that the function delete_ride correctly handles a ride_id of valid type and value,
        with a valid return from the db function delete_ride_by_id.
        """
        example_ride = {
            "user_id" : 1234,
            "bike_id": 1,
            "start_time" : datetime.strptime("07/03/2021 16:21:12", "%d/%m/%Y %H:%M:%S")
            }
        mock_db_delete_ride.return_value = example_ride

        assert delete_ride(None, 1) == (example_ride, 200)
        mock_db_delete_ride.assert_called_once_with(None, 1)
