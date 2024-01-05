"""
Dashboard script for recent rides to establish connection to the RDS database, fetch data using SQL queries and
create visualisations in a Streamlit app (using functions from the `database.py`,
`visualisations.py` and `utilities.pyz files as necessary).
"""

from datetime import datetime
import time

from dotenv import load_dotenv
from psycopg2 import extensions
import streamlit as st

from database import (get_database_connection, get_recent_12hr_data,
                      get_current_ride_data, get_current_ride_data_highest)
from visualisations import (get_dashboard_title, get_last_updated_current_ride,
                            get_recent_rides_header, get_last_updated_recent_rides)


RECENT_RIDE_REFRESH_RATE = 5
LAST_UPDATED_COUNT_INCREMENT = 1


def main_recent_rides(db_connection: extensions.connection) -> None:
    """
    Main function that calls all the functions related to
    displaying the recent rides visualisations.
    """
    with st.container():
        get_recent_rides_header()

        recent_rides = get_recent_12hr_data(db_connection)

        # Placeholder for last updated time caption
        empty_last_updated_placeholder = st.empty()

        return empty_last_updated_placeholder


if __name__ == "__main__":

    load_dotenv()

    conn = get_database_connection()

    get_dashboard_title()

    while True:
        # Auto-refresh the page
        last_updated_placeholder_recent = main_recent_rides(conn)
        update_time = datetime.now()

        for i in range(RECENT_RIDE_REFRESH_RATE):
            get_last_updated_current_ride(
                update_time, last_updated_placeholder_recent)
            time.sleep(LAST_UPDATED_COUNT_INCREMENT)

        st.rerun()
