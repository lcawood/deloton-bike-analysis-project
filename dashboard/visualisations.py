"""
Functions for visualising Deloton Bike ride data on the streamlit app.
"""

from datetime import datetime

import pandas as pandas
import streamlit as st
import altair as alt


def get_dashboard_title() -> None:
    """Returns a title for the dashboard."""
    st.title("Deloton Bike Analysis")


def get_current_ride_header(rider_name: str) -> None:
    """Returns a header for the current ride and the rider's name."""
    st.header(f"CURRENT RIDE: {rider_name}", divider='blue')


def calculate_age(birthdate: datetime, current_date: datetime = datetime.utcnow()) -> int:
    """
    Returns the age in years for the given date as a datetime object
    in the format YYYY-MM-DD.
    'birthdate' is assumed to be cleaned and always as a datetime type.
    """
    age = current_date.year - birthdate.year - \
        ((current_date.month, current_date.day)
         < (birthdate.month, birthdate.day))

    return age


def get_current_ride_header_metrics(user_details) -> None:
    """
    Gets the main headers and displays them.
    """

    # SELECT first_name, last_name, height, weight, gender,
    # heart_rate, power, resistance, elapsed_time
    gender = user_details[4]
    gender_emoji = ":male_sign:" if gender == "male" else ":female_sign:"
    # age =
    # height =
    # weight =
    head_cols = st.columns(4)
    with head_cols[0]:
        st.metric(f"{gender_emoji}{user_details[4]}")
    with head_cols[1]:
        st.metric("Age")
    with head_cols[2]:
        st.metric("Height")
    with head_cols[3]:
        st.metric("Weight")
    st.divider()
