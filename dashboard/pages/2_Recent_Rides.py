"""
Dashboard script for recent rides to establish connection to the RDS database,
fetch data using SQL queries and create visualisations in a Streamlit app
using functions from the `database.py`, `visualisations.py` and `utilities.pyz files as necessary.
"""

# 'Unable to import' errors
# pylint: disable = E0401

from datetime import datetime
import time

import altair as alt
from dotenv import load_dotenv
import pandas as pd
from psycopg2 import extensions
import streamlit as st

from database import (get_database_connection, get_recent_12hr_data)
from utilities import (process_dataframe)
from visualisations import (get_dashboard_title, get_total_ride_count_age_bar_chart,
                            get_recent_rides_header, get_last_updated_recent_rides,
                            get_total_duration_gender_bar_chart,
                            get_total_ride_count_gender_bar_chart,
                            get_power_output_avg_line_chart,
                            get_resistance_output_avg_line_chart,
                            get_power_output_cumul_line_chart,
                            get_resistance_output_cumul_line_chart)


RECENT_RIDE_REFRESH_RATE = 20
LAST_UPDATED_COUNT_INCREMENT = 1


def generate_bar_charts(recent_rides: pd.DataFrame, selector_gender) -> None:
    """Generates the bar charts for the dashboard."""

    total_duration_gender_chart = get_total_duration_gender_bar_chart(
        recent_rides, selector_gender)

    ride_count_by_gender_chart = get_total_ride_count_gender_bar_chart(
        recent_rides, selector_gender)

    ride_count_by_age_chart = get_total_ride_count_age_bar_chart(
        recent_rides, selector_gender)

    row = total_duration_gender_chart | ride_count_by_gender_chart | ride_count_by_age_chart

    return row


def generate_line_charts(recent_rides: pd.DataFrame, selector_gender) -> None:
    """Generates the line charts for the dashboard."""

    avg_power_chart = get_power_output_avg_line_chart(
        recent_rides, selector_gender)

    avg_resistance_chart = get_resistance_output_avg_line_chart(
        recent_rides, selector_gender)

    cumul_power_chart = get_power_output_cumul_line_chart(
        recent_rides, selector_gender)

    cumul_resistance_chart = get_resistance_output_cumul_line_chart(
        recent_rides, selector_gender)

    # try to add empty bar joining to space horizontally

    row1 = avg_power_chart | avg_resistance_chart
    row2 = cumul_power_chart | cumul_resistance_chart
    line_graphs = row1 & row2

    return line_graphs


def main_recent_rides(db_connection: extensions.connection) -> None:
    """
    Main function that calls all the functions related to
    displaying the recent rides visualisations.
    """

    with st.sidebar:
        st.subheader("Date resolution")
        date_resolution = st.slider('Select a resolution (minutes):',
                                    1, 60, 10)

    with st.container():

        get_recent_rides_header()

        recent_rides = get_recent_12hr_data(db_connection)
        recent_rides = process_dataframe(recent_rides, date_resolution)

        # placeholder for last updated time caption
        empty_last_updated_placeholder = st.empty()

        # create selectors
        selector_gender = alt.selection_single(
            fields=['gender'], empty='all', name='GenderSelector')
        selector_age = alt.selection_single(
            fields=['age'], empty='all', name='AgeSelector')

        bar_charts = generate_bar_charts(
            recent_rides, selector_gender)

        line_charts = generate_line_charts(recent_rides, selector_gender)

        widget = bar_charts & line_charts

        st.altair_chart(widget)

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
