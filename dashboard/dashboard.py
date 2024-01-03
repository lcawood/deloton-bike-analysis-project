"""
Dashboard script to establish connection to the RDS database, fetch data using SQL queries and
create visualisations in a Streamlit app (using functions from the `database.py`, `visualisations.py` and `utilities.py`
files as necessary).
"""


from os import environ

from dotenv import load_dotenv
from pandas import DataFrame
import streamlit as st

from database import get_database_connection, get_current_ride_data
from utilities import get_current_rider_name
from visualisations import get_current_ride_header


if __name__ == "__main__":

    load_dotenv()

    conn = get_database_connection()

    # current_ride = get_current_ride_data(conn)
    current_ride = ["John", "Doe", 175, 75, "Male", 105, 11.4, 60, 45]

    # SELECT first_name, last_name, height, weight, gender,
    # heart_rate, power, resistance, elapsed_time

    print(current_ride)

    rider_name = get_current_rider_name(current_ride)

    get_current_ride_header(rider_name)
