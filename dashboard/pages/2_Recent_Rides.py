"""
Dashboard script for recent rides to establish connection to the RDS database, fetch data using SQL queries and
create visualisations in a Streamlit app (using functions from the `database.py`,
`visualisations.py` and `utilities.pyz files as necessary).
"""

from datetime import datetime
import time

import altair as alt
from dotenv import load_dotenv
import pandas as pd
from psycopg2 import extensions
import streamlit as st

from database import (get_database_connection, get_recent_12hr_data,
                      get_ride_count_gender, get_ride_count_age)
from utilities import (process_dataframe,
                       get_dataframe_columns_for_line_charts, process_dataframe_power_output_avg,
                       process_dataframe_resistance_output_avg, process_dataframe_power_output_cumul,
                       process_dataframe_resistance_output_cumul)
from visualisations import (get_dashboard_title, get_total_ride_count_age_bar_chart,
                            get_recent_rides_header, get_last_updated_recent_rides,
                            get_total_duration_gender_bar_chart, get_total_ride_count_gender_bar_chart,
                            get_power_output_avg_line_chart, get_resistance_output_avg_line_chart,
                            get_power_output_cumul_line_chart, get_resistance_output_cumul_line_chart)


RECENT_RIDE_REFRESH_RATE = 20
LAST_UPDATED_COUNT_INCREMENT = 1


def generate_bar_charts(recent_rides: pd.DataFrame, selector) -> None:
    """Generates the bar charts for the dashboard."""

    total_duration_gender_chart = get_total_duration_gender_bar_chart(
        recent_rides, selector)

    ride_count_by_gender_chart = get_total_ride_count_gender_bar_chart(
        recent_rides, selector)

    ride_count_by_age_chart = get_total_ride_count_age_bar_chart(
        recent_rides, selector)

    row = total_duration_gender_chart | ride_count_by_gender_chart | ride_count_by_age_chart

    st.altair_chart(row)


def generate_line_charts(avg_power_over_time: pd.DataFrame,
                         avg_resistance_over_time: pd.DataFrame,
                         cumul_power_over_time: pd.DataFrame,
                         cumul_resistance_over_time: pd.DataFrame) -> None:
    """Generates the line charts for the dashboard."""

    line_col_11, line_col_12 = st.columns(2, gap='large')
    with line_col_11:
        avg_power_chart = get_power_output_avg_line_chart(
            avg_power_over_time)

        st.altair_chart(avg_power_chart,
                        use_container_width=True)

    with line_col_12:
        avg_resistance_chart = get_resistance_output_avg_line_chart(
            avg_resistance_over_time)

        st.altair_chart(avg_resistance_chart,
                        use_container_width=True)

    line_col_21, line_col_22 = st.columns(2, gap='large')
    with line_col_21:
        cumul_power_chart = get_power_output_cumul_line_chart(
            cumul_power_over_time)

        st.altair_chart(cumul_power_chart,
                        use_container_width=True)

    with line_col_22:
        cumul_resistance_chart = get_resistance_output_cumul_line_chart(
            cumul_resistance_over_time)

        st.altair_chart(cumul_resistance_chart,
                        use_container_width=True)


def timestamp(t):
    return pd.to_datetime(t).timestamp() * 1000


def main_recent_rides(db_connection: extensions.connection) -> None:
    """
    Main function that calls all the functions related to
    displaying the recent rides visualisations.
    """

    gender_select = alt.selection_single(fields=["gender"], empty=False)
    age_select = alt.selection_single(fields=["age_bracket"], empty=False)

    with st.sidebar:
        st.subheader("Date resolution")
        date_resolution = st.slider('Select a resolution (minutes):',
                                    1, 60, 10)

    with st.container():

        get_recent_rides_header()

        recent_rides = get_recent_12hr_data(db_connection)
        recent_rides = process_dataframe(recent_rides, date_resolution)

        print(recent_rides['gender'])
        # ride_count_by_gender = get_ride_count_gender(db_connection)
        # ride_count_by_age = get_ride_count_age(db_connection)
        # line_chart_data = get_dataframe_columns_for_line_charts(
        #     recent_rides, date_resolution)

        # # # average charts
        # avg_power_over_time = process_dataframe_power_output_avg(
        #     recent_rides)
        # avg_resistance_over_time = process_dataframe_resistance_output_avg(
        #     line_chart_data)

        # # cumulative charts
        # cumul_power_over_time = process_dataframe_power_output_cumul(
        #     line_chart_data)
        # cumul_resistance_over_time = process_dataframe_resistance_output_cumul(
        #     line_chart_data)

        # placeholder for last updated time caption
        empty_last_updated_placeholder = st.empty()

        # create selectors
        selector_gender = alt.selection_single(
            fields=['gender'], empty='all', name='GenderSelector')
        selector_age = alt.selection_single(
            fields=['age'], empty='all', name='AgeSelector')

        generate_bar_charts(
            recent_rides, selector_gender)

        # generate_line_charts(avg_power_over_time,
        #                      avg_resistance_over_time, cumul_power_over_time, cumul_resistance_over_time)

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
