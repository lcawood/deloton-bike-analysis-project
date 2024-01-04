"""
Dashboard script to establish connection to the RDS database, fetch data using SQL queries and
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
from utilities import get_current_rider_name, is_heart_rate_abnormal
from visualisations import (get_current_ride_header, get_dashboard_title,
                            get_current_ride_header_personal_info, get_current_ride_metrics,
                            get_current_ride_personal_best_metrics, get_last_updated_current_ride,
                            get_heart_rate_warning, get_recent_rides_header, get_last_updated_recent_rides)


CURRENT_RIDE_REFRESH_RATE = 5
RECENT_RIDE_REFRESH_RATE = 5
LAST_UPDATED_COUNT_INCREMENT = 1


def main_current_ride(db_connection: extensions.connection) -> None:
    """
    Main function that calls all the functions related to
    displaying the current ride visualisations.
    """
    with st.container():
        current_ride = get_current_ride_data(db_connection)
        current_ride_personal_best = get_current_ride_data_highest(
            db_connection, current_ride)

        rider_name = get_current_rider_name(current_ride)

        get_current_ride_header(rider_name)

        # Placeholder for last updated time caption
        empty_last_updated_placeholder = st.empty()

        get_current_ride_header_personal_info(current_ride)

        get_current_ride_metrics(current_ride)

        if is_heart_rate_abnormal(current_ride):
            heart_rate = current_ride[7]
            get_heart_rate_warning(heart_rate)

        get_current_ride_personal_best_metrics(current_ride_personal_best)

        return empty_last_updated_placeholder


def main_recent_rides(db_connection: extensions.connection) -> None:
    """
    Main function that calls all the functions related to
    displaying the recent rides visualisations.
    """
    with st.container():
        get_recent_rides_header()

        recent_rides = get_recent_12hr_data(db_connection)
        print(recent_rides)

        # Placeholder for last updated time caption
        empty_last_updated_placeholder = st.empty()

        return empty_last_updated_placeholder


if __name__ == "__main__":

    load_dotenv()

    conn = get_database_connection()

    get_dashboard_title()
    try:
        while True:
            # Auto-refresh the current ride section
            last_updated_placeholder_current = main_current_ride(conn)
            update_time = datetime.now()

            last_updated_placeholder_recent = main_recent_rides(conn)
            update_time = datetime.now()

            for i in range(CURRENT_RIDE_REFRESH_RATE):
                get_last_updated_current_ride(
                    update_time, last_updated_placeholder_current)
                get_last_updated_current_ride(
                    update_time, last_updated_placeholder_recent)
                time.sleep(LAST_UPDATED_COUNT_INCREMENT)

            st.rerun()
    finally:
        conn.close()
