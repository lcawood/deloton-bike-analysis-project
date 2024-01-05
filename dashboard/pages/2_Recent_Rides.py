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
                      get_ride_count)
from utilities import (process_dataframe_types)
from visualisations import (get_dashboard_title, get_last_updated_current_ride,
                            get_recent_rides_header, get_last_updated_recent_rides,
                            get_total_duration_gender_bar_chart, get_total_ride_count_gender_bar_chart)


RECENT_RIDE_REFRESH_RATE = 3
LAST_UPDATED_COUNT_INCREMENT = 1


def main_recent_rides(db_connection: extensions.connection) -> None:
    """
    Main function that calls all the functions related to
    displaying the recent rides visualisations.
    """
    with st.container():
        get_recent_rides_header()

        recent_rides = get_recent_12hr_data(db_connection)
        recent_rides = process_dataframe_types(recent_rides)
        ride_count_by_gender = get_ride_count(db_connection)

        print(ride_count_by_gender)

        # Placeholder for last updated time caption
        empty_last_updated_placeholder = st.empty()

        col1, col2, col3 = st.columns([1, 1, 3], gap='large')
        with col1:
            total_duration_gender_chart = get_total_duration_gender_bar_chart(
                recent_rides)
            st.altair_chart(total_duration_gender_chart,
                            use_container_width=True)

        with col2:
            ride_count_by_gender_chart = get_total_ride_count_gender_bar_chart(
                ride_count_by_gender)
            st.altair_chart(ride_count_by_gender_chart,
                            use_container_width=True)

        return empty_last_updated_placeholder


if __name__ == "__main__":

    st.set_page_config(
        page_title="Recent Rides",
        page_icon="🚲",
        layout="wide"
    )

    load_dotenv()

    conn = get_database_connection()

    get_dashboard_title()

    while True:
        # Auto-refresh the page
        last_updated_placeholder_recent = main_recent_rides(conn)
        update_time = datetime.now()

        for i in range(RECENT_RIDE_REFRESH_RATE):
            get_last_updated_recent_rides(
                update_time, last_updated_placeholder_recent)
            time.sleep(LAST_UPDATED_COUNT_INCREMENT)

        st.rerun()
