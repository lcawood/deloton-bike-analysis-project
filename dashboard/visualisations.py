"""
Functions for visualising Deloton Bike ride data on the streamlit app.
"""

from datetime import datetime

import pandas as pandas
import streamlit as st
import altair as alt

from utilities import calculate_age


def get_dashboard_title() -> None:
    """Returns a title for the dashboard."""
    st.title("Deloton Bike Analysis")


def get_current_ride_header(rider_name: str) -> None:
    """Returns a header for the current ride and the rider's name."""
    st.header(f"CURRENT RIDE: {rider_name}", divider='blue')


def get_current_ride_header_metrics(user_details) -> None:
    """
    Gets the main headers for the current ride and displays them.
    """

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
        st.metric("Gender", f"{gender_emoji} {gender}")
    with head_cols[1]:
        st.metric("Age", age)
    with head_cols[2]:
        st.metric("Height", height)
    with head_cols[3]:
        st.metric("Weight", weight)
    st.divider()

    # with st.expander('Show graph.'):
