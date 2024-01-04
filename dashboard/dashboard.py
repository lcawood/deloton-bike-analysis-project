"""
Dashboard script to establish connection to the RDS database, fetch data using SQL queries and
create visualisations in a Streamlit app (using functions from the `database.py`, `visualisations.py` and `utilities.py`
files as necessary).
"""

from datetime import datetime
from os import environ

from dotenv import load_dotenv
from pandas import DataFrame
from psycopg2 import extensions
import streamlit as st

from database import (get_database_connection,
                      get_current_ride_data, get_current_ride_data_highest)
from utilities import get_current_rider_name
from visualisations import (
    get_current_ride_header, get_dashboard_title, get_current_ride_header_personal_info, get_current_ride_metrics, get_current_ride_personal_best_metrics)


def main_current_ride(db_connection: extensions.connection) -> None:
    """
    Main function that calls all the functions related to
    displaying the current ride visualisations.
    """

    current_ride = get_current_ride_data(conn)
    current_ride_personal_best = get_current_ride_data_highest(
        conn, current_ride)

    rider_name = get_current_rider_name(current_ride)

    get_dashboard_title()

    get_current_ride_header(rider_name)

    get_current_ride_header_personal_info(current_ride)

    get_current_ride_metrics(current_ride)

    get_current_ride_personal_best_metrics(current_ride_personal_best)


if __name__ == "__main__":

    load_dotenv()

    conn = get_database_connection()

    fake_birthdate = datetime.strptime('1999-01-01', "%Y-%m-%d")
    current_ride = get_current_ride_data(conn)
    current_ride_personal_best = get_current_ride_data_highest(
        conn, current_ride)

    rider_name = get_current_rider_name(current_ride)

    get_dashboard_title()

    get_current_ride_header(rider_name)

    get_current_ride_header_personal_info(current_ride)

    get_current_ride_metrics(current_ride)

    get_current_ride_personal_best_metrics(current_ride_personal_best)
