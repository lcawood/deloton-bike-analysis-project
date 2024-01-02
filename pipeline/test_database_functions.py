"""Tests the database functions work as intended. Ensures they correctly query the database."""
from unittest.mock import MagicMock

from database_functions import load_user_into_database,load_address_into_database,select_address_from_database,load_ride_into_database,select_ride_from_database,load_reading_into_database,select_reading_from_database

EXAMPLE_USER = {'user_id' : 1234,'address_id' : 6,'first_name': "Charlie",
                          'last_name': "Derick",'birthdate': "2002-04-13",
                          'height' : 152,'weight' : 280,'email' : "charlie@gmail.com",
                          'gender' : "male",'account_created' : "2021-09-27"}

EXAMPLE_ADDRESS = {"first_line" : "63 Studio","second_line" : "Nursery Avenue", "city" : "London", "postcode" : "LA1 34A"}

EXAMPLE_RIDE = {"user_id" : 1234, "bike_id": 1, "start_time" : "2021-07-03 16:21:12"}

def test_load_address_into_database():
    """Tests that a address gets correctly loaded into the database"""

    mock_conn = MagicMock()

    mock_execute = mock_conn.cursor.return_value\
            .__enter__.return_value\
            .execute
    mock_fetch = mock_conn.cursor.return_value\
            .__enter__.return_value\
            .fetchone
    
    mock_fetch.return_value = (1,)

    address_id = load_address_into_database(mock_conn, EXAMPLE_ADDRESS)

    mock_execute.assert_called_once()
    mock_fetch.assert_called_once()

    assert address_id == 1

def test_select_address_from_database():
    """Tests that a address gets correctly selected from the database"""

    mock_conn = MagicMock()

    mock_execute = mock_conn.cursor.return_value\
            .__enter__.return_value\
            .execute
    mock_fetch = mock_conn.cursor.return_value\
            .__enter__.return_value\
            .fetchone
    
    mock_fetch.return_value = (1,)

    address_id = select_address_from_database(mock_conn, EXAMPLE_ADDRESS)

    mock_execute.assert_called_once()
    mock_fetch.assert_called_once()

    assert address_id == 1


def test_load_user_into_database():
    """Tests that a user gets correctly loaded into the database"""

    mock_conn = MagicMock()

    mock_execute = mock_conn.cursor.return_value\
            .__enter__.return_value\
            .execute
    mock_fetch = mock_conn.cursor.return_value\
            .__enter__.return_value\
            .fetchone
    
    mock_fetch.return_value = (EXAMPLE_USER["user_id"],)

    user_id = load_user_into_database(mock_conn, EXAMPLE_USER)

    mock_execute.assert_called_once()
    mock_fetch.assert_called_once()

    assert user_id == 1234

def test_load_ride_into_database():
    """Tests that a ride gets correctly loaded into the database"""

    mock_conn = MagicMock()

    mock_execute = mock_conn.cursor.return_value\
            .__enter__.return_value\
            .execute
    mock_fetch = mock_conn.cursor.return_value\
            .__enter__.return_value\
            .fetchone
    
    mock_fetch.return_value = (1,)

    ride_id = load_ride_into_database(mock_conn, EXAMPLE_RIDE)

    mock_execute.assert_called_once()
    mock_fetch.assert_called_once()

    assert ride_id == 1

def test_select_ride_from_database():
    """Tests that a ride gets correctly selected from the database"""

    mock_conn = MagicMock()

    mock_execute = mock_conn.cursor.return_value\
            .__enter__.return_value\
            .execute
    mock_fetch = mock_conn.cursor.return_value\
            .__enter__.return_value\
            .fetchone
    
    mock_fetch.return_value = (1,)

    ride_id = select_ride_from_database(mock_conn, EXAMPLE_RIDE)

    mock_execute.assert_called_once()
    mock_fetch.assert_called_once()

    assert ride_id == 1