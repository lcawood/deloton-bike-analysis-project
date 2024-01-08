"""Functions for visualising Deloton Bike ride data on the Streamlit app."""

from datetime import datetime, timedelta

import altair as alt
import pandas as pd
import streamlit as st

from utilities import calculate_age
from database import (get_recent_12hr_data)


def get_dashboard_title() -> None:
    """Generates a title for the dashboard."""
    st.title("Deloton Bike Analysis")


# CURRENT RIDE
# @st.cache_data(show_spinner="Retrieving current ride...")
def get_current_ride_header(rider_name: str) -> None:
    """Generates a header for the current ride and the rider's name."""
    st.header(f"CURRENT RIDE: {rider_name}", divider='blue')


def get_last_updated_current_ride(last_update_time: datetime,
                                  _last_updated_placeholder: st.empty) -> None:
    """Generates a caption under the header with the time since the last data update."""

    current_time = datetime.utcnow()
    time_delta = int((current_time - last_update_time).total_seconds())

    _last_updated_placeholder.caption(
        f"Last updated: {time_delta} seconds ago")


# @st.cache_data(show_spinner="Retrieving personal info...")
def get_current_ride_header_personal_info(user_details: list) -> None:
    """
    Gets the main header personal_info for the current ride and displays them.
    """
    with st.expander('Personal Info ⛛'):
        # get metrics
        height = user_details[3]
        weight = user_details[4]
        gender = user_details[5]
        gender_emoji = "♂" if gender == "male" else "♀"
        birthdate = user_details[6]
        age = calculate_age(birthdate)

        # create visualisation
        head_cols = st.columns(4)
        with head_cols[0]:
            st.metric("Gender", f"{gender_emoji} {gender.title()}")
        with head_cols[1]:
            st.metric("Age", age)
        with head_cols[2]:
            st.metric("Height", f"{height} cm")
        with head_cols[3]:
            st.metric("Weight", f"{weight} kg")


def get_heart_rate_warning(heart_rate: int) -> None:
    """Displays a warning message if the heart rate is abnormal for the current ride."""
    if heart_rate == 0:
        st.warning(
            """WARNING! HEART RATE IS ZERO: \n
            PLEASE PLACE HANDS ON HANDLE BARS OR SEEK ASSISTANCE!""", icon="⚠️")
    else:
        st.warning(
            """WARNING! HEART RATE IS OUTSIDE THE HEALTHY RANGE: \n
            PLEASE SLOW DOWN OR SEEK ASSISTANCE!""", icon="⚠️")


# @st.cache_data(show_spinner="Retrieving ride metrics...")
def get_current_ride_metrics(user_details: list) -> None:
    """
    Generates the header metrics for the current ride and displays them.
    """

    # get metrics
    heart_rate = user_details[7]
    power = round(user_details[8], 1)
    resistance = user_details[9]
    elapsed_time = user_details[10]

    # create visualisation
    head_cols = st.columns(4)
    with head_cols[0]:
        st.metric("Elapsed Time", f"{elapsed_time} secs")
    with head_cols[1]:
        st.metric("Heart Rate", f"{heart_rate} BPM")
    with head_cols[2]:
        st.metric("Power", f"{power} W")
    with head_cols[3]:
        st.metric("Resistance", resistance)


# @st.cache_data(show_spinner="Retrieving personal bests...")
def get_current_ride_personal_best_metrics(user_best_details: list) -> None:
    """
    Generates the main header metric personal bests for the current ride and displays them.
    """
    with st.expander('Personal Best ⛛'):
        get_current_ride_metrics(user_best_details)


# RECENT RIDES
# @st.cache_data(show_spinner="Retrieving recent rides...")
def get_recent_rides_header() -> None:
    """Generates a header for the recent rides section."""
    st.header(f"RECENT RIDES", divider='blue')


def get_last_updated_recent_rides(last_update_time: datetime,
                                  _last_updated_placeholder: st.empty) -> None:
    """Generates a caption under the header with the time since the last data update."""

    # time_delta = (current_time-last_update_time).total_seconds()

    current_time = datetime.utcnow()
    time_delta = int((current_time - last_update_time).total_seconds())

    _last_updated_placeholder.caption(
        f"Last updated: {time_delta} seconds ago")


def get_total_duration_gender_bar_chart(recent_data: pd.DataFrame, selector) -> alt.Chart:
    """
    Generates a bar chart for the total elapsed_time grouped by gender
    over the past 12 hours.
    """

    chart = alt.Chart(recent_data, title='Total Duration (by gender)').transform_aggregate(
        total_elapsed_time='sum(elapsed_time)',
        groupby=['gender']
    ).transform_calculate(
        total_elapsed_time_hours='datum.total_elapsed_time / 3600'
    ).mark_bar().encode(
        x=alt.X('gender:N', title='Gender'),
        y=alt.Y('total_elapsed_time_hours:Q',
                title='Total Elapsed Time (hours)'),
        tooltip=[alt.Tooltip('gender:N', title='Gender'), alt.Tooltip(
            'total_elapsed_time_hours:Q', title='Total Elapsed Time')],
        opacity=alt.condition(selector, alt.value(1), alt.value(0.25))
    ).add_selection(selector).transform_filter(selector).properties(width=300)

    return chart


def get_total_ride_count_gender_bar_chart(recent_rides: pd.DataFrame, selector) -> alt.Chart:
    """
    Generates a bar chart for the total number of rides grouped by gender
    over the past 12 hours.
    """

    chart = alt.Chart(recent_rides, title='Total Number of rides (by gender)').mark_bar().encode(
        x=alt.X('gender:N', title='Gender'),
        y=alt.Y('count():Q', title='Number of Rides'),
        opacity=alt.condition(selector, alt.value(1), alt.value(0.25))
    ).add_selection(selector).transform_filter(selector).properties(width=300)

    return chart


def get_total_ride_count_age_bar_chart(ride_counts: pd.DataFrame, selector) -> alt.Chart:
    """
    Generates a bar chart for the total number of rides grouped by age brackets
    over the past 12 hours.
    """

    chart = alt.Chart(ride_counts, title='Total Number of rides (by age)').mark_bar().encode(
        x=alt.X('age_bracket:N', title='Ages'),
        y=alt.Y('count():Q', title='Number of Rides'),
        opacity=alt.condition(selector, alt.value(1), alt.value(0.25))
    ).add_selection(selector).transform_filter(selector).properties(width=1000)

    return chart


def get_power_output_avg_line_chart(recent_data: pd.DataFrame) -> alt.Chart:
    """Generates a line chart for the average power output over the past 12 hours."""

    chart = alt.Chart(recent_data, title='Average Power Output').mark_line().encode(
        x=alt.X('reading_time:T', axis=alt.Axis(title='Time')),
        y=alt.Y('power:Q', title='Average Power (W)'),
    )

    return chart


def get_resistance_output_avg_line_chart(recent_data: pd.DataFrame) -> alt.Chart:
    """Generates a line chart for the average resistance output over the past 12 hours."""

    chart = alt.Chart(recent_data, title='Average Resistance output').mark_line().encode(
        x=alt.X('reading_time:T', axis=alt.Axis(title='Time')),
        y=alt.Y('resistance:Q', title='Average Resistance'),
    )

    return chart


def get_power_output_cumul_line_chart(recent_data: pd.DataFrame) -> alt.Chart:
    """Generates a line chart for the cumulative power output over the past 12 hours."""

    chart = alt.Chart(recent_data, title='Cumulative Power Output').mark_line().encode(
        x=alt.X('reading_time:T', axis=alt.Axis(title='Time')),
        y=alt.Y('power:Q', title='Cumulative Power (W)'),
    )

    return chart


def get_resistance_output_cumul_line_chart(recent_data: pd.DataFrame) -> alt.Chart:
    """Generates a line chart for the cumulative resistance output over the past 12 hours."""

    chart = alt.Chart(recent_data, title='Cumulative Resistance output').mark_line().encode(
        x=alt.X('reading_time:T', axis=alt.Axis(title='Time')),
        y=alt.Y('resistance:Q', title='Cumulative Resistance'),
    )

    return chart
