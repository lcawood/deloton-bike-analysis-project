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
                      get_ride_count_gender, get_ride_count_age)
from utilities import (process_dataframe_types,
                       get_dataframe_columns_for_line_charts, process_dataframe_power_output_avg,
                       process_dataframe_resistance_output_avg, process_dataframe_power_output_cumul,
                       process_dataframe_resistance_output_cumul)
from visualisations import (get_dashboard_title, get_total_ride_count_age_bar_chart,
                            get_recent_rides_header, get_last_updated_recent_rides,
                            get_total_duration_gender_bar_chart, get_total_ride_count_gender_bar_chart,
                            get_power_output_avg_line_chart,)


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
        ride_count_by_gender = get_ride_count_gender(db_connection)
        ride_count_by_age = get_ride_count_age(db_connection)
        line_chart_data = get_dataframe_columns_for_line_charts(recent_rides)

        print(line_chart_data['reading_time'].sort_values(ascending=True))
        avg_power_over_time = process_dataframe_power_output_avg(
            line_chart_data)
        avg_resistance_over_time = process_dataframe_resistance_output_avg
        cumul_power_over_time = process_dataframe_power_output_cumul
        cumul_resistance_over_time = process_dataframe_resistance_output_cumul

        # Placeholder for last updated time caption
        empty_last_updated_placeholder = st.empty()

        # Generate bar charts
        bar_col_1, bar_col_2, bar_col_3 = st.columns([1, 1, 2], gap='large')
        with bar_col_1:

            total_duration_gender_chart = get_total_duration_gender_bar_chart(
                recent_rides)
            st.altair_chart(total_duration_gender_chart,
                            use_container_width=True)

        with bar_col_2:

            ride_count_by_gender_chart = get_total_ride_count_gender_bar_chart(
                ride_count_by_gender)
            st.altair_chart(ride_count_by_gender_chart,
                            use_container_width=True)

        with bar_col_3:

            ride_count_by_age_chart = get_total_ride_count_age_bar_chart(
                ride_count_by_age)

            st.altair_chart(ride_count_by_age_chart,
                            use_container_width=True)

        # Generate line charts
        line_col_1, line_col_2 = st.columns(2, gap='large')
        with line_col_1:
            avg_power_chart = get_power_output_avg_line_chart(
                avg_power_over_time)

            st.altair_chart(avg_power_chart,
                            use_container_width=True)
        # with line_col_2:

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
