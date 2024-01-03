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

from visualisations import get_current_ride_name


def get_current_ride_header() -> None:
    """Gets the current ride header and name of the current rider."""
    st.header(f"CURRENT RIDE: ")


if __name__ == "__main__":

    load_dotenv()

    conn = get_database_connection()

    current_ride = get_current_ride_data(conn)

    print(current_ride)

    rider_name =
