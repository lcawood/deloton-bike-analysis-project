"""
Dashboard script to establish connection to the RDS database, fetch data using SQL queries and
create visualisations in a Streamlit app (using functions from the `database.py`, `visualisations.py` and `utilities.py`
files as necessary).
"""

from datetime import datetime
import time

from dotenv import load_dotenv
from psycopg2 import extensions
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from database import (get_database_connection,
                      get_current_ride_data, get_current_ride_data_highest)
from utilities import get_current_rider_name
from visualisations import (get_current_ride_header, get_dashboard_title,
                            get_current_ride_header_personal_info, get_current_ride_metrics,
                            get_current_ride_personal_best_metrics, get_last_updated_current_ride)


CURRENT_RIDE_REFRESH_RATE = 20
LAST_UPDATED_COUNT_INCREMENT = 1


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

    # Placeholder for last updated time caption
    last_updated_placeholder = st.empty()

    get_current_ride_metrics(current_ride)

    get_current_ride_personal_best_metrics(current_ride_personal_best)

    return last_updated_placeholder


if __name__ == "__main__":

    load_dotenv()

    conn = get_database_connection()

    while True:
        # Auto-refresh the current ride section
        last_updated_placeholder = main_current_ride(conn)
        update_time = datetime.now()
        for i in range(CURRENT_RIDE_REFRESH_RATE):
            get_last_updated_current_ride(
                update_time, last_updated_placeholder)
            time.sleep(LAST_UPDATED_COUNT_INCREMENT)

        st.rerun()
