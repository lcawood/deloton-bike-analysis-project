"""Tests the database functions work as intended. Ensures they correctly query the database."""

from database_functions import load_user_into_database,load_address_into_database,select_address_from_database,load_ride_into_database,select_ride_from_database,load_reading_into_database,select_reading_from_database

EXAMPLE_USER = {'user_id' : 1234,'address_id' : 6,'first_name': "Charlie",
                          'last_name': "Derick",'birthdate': "2002-04-13",
                          'height' : 152,'weight' : 280,'email' : "charlie@gmail.com",
                          'gender' : "male",'account_created' : "2021-09-27"}

EXAMPLE_ADDRESS = {"first_line" : "63 Studio","second_line" : "Nursery Avenue", "city" : "London", "postcode" : "LA1 34A"}

def test_load_address_into_database():
    """Tests that a address gets correctly loaded into the database"""