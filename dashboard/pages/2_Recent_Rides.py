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
from streamlit_dynamic_filters import DynamicFilters

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


def generate_bar_charts(recent_rides: pd.DataFrame, selector_gender, selector_age) -> None:
    """Generates the bar charts for the dashboard."""

    total_duration_gender_chart = get_total_duration_gender_bar_chart(
        recent_rides, selector_gender, selector_age)

    ride_count_by_gender_chart = get_total_ride_count_gender_bar_chart(
        recent_rides, selector_gender, selector_age)

    ride_count_by_age_chart = get_total_ride_count_age_bar_chart(
        recent_rides, selector_gender, selector_age)

    widget_top_row = alt.hconcat(
        total_duration_gender_chart,
        ride_count_by_gender_chart,
        ride_count_by_age_chart,
        spacing=100
    )

    return widget_top_row


def generate_line_charts(recent_rides: pd.DataFrame, selector_gender, selector_age) -> None:
    """Generates the line charts for the dashboard."""

    avg_power_chart = get_power_output_avg_line_chart(
        recent_rides, selector_gender, selector_age)

    avg_resistance_chart = get_resistance_output_avg_line_chart(
        recent_rides, selector_gender, selector_age)

    cumul_power_chart = get_power_output_cumul_line_chart(
        recent_rides, selector_gender, selector_age)

    cumul_resistance_chart = get_resistance_output_cumul_line_chart(
        recent_rides, selector_gender, selector_age)

    # Join graphs for widget

    widget_mid_row = alt.hconcat(
        avg_power_chart, avg_resistance_chart, spacing=50)
    widget_bot_row = alt.hconcat(
        cumul_power_chart, cumul_resistance_chart, spacing=50)
    widget_line_graphs = alt.vconcat(
        widget_mid_row, widget_bot_row, spacing=50)

    return widget_line_graphs


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

        # get recent rides data
        recent_rides = get_recent_12hr_data(db_connection)
        recent_rides = process_dataframe(recent_rides, date_resolution)

        # generate sidebar filters
        dynamic_filters = DynamicFilters(
            recent_rides, filters=['age_bracket', 'gender'])

        with st.sidebar:
            st.write("Apply filters in any order:")

        dynamic_filters.display_filters(location='sidebar')

        filtered_data = dynamic_filters.filter_df()

        # placeholder for last updated time caption
        empty_last_updated_placeholder = st.empty()

        # create selectors
        selector_gender = alt.selection_single(
            fields=['gender'], empty='all', name='GenderSelector')
        selector_age = alt.selection_single(
            fields=['age_bracket'], empty='all', name='AgeSelector')

        # generate charts
        bar_charts = generate_bar_charts(
            filtered_data, selector_gender, selector_age)

        line_charts = generate_line_charts(
            filtered_data, selector_gender, selector_age)

        # concatenate charts in widget to allow interactive filtering
        widget = alt.vconcat(bar_charts, line_charts,
                             spacing=75).configure_axis(gridColor='#6ecc89', gridOpacity=0.3)

        # possible colours: darkseagreen,

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
