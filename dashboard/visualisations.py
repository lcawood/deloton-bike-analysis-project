"""Functions for visualising Deloton Bike ride data on the Streamlit app."""

from datetime import datetime

import pandas as pd
import streamlit as st
import altair as alt

from utilities import calculate_age, is_heart_rate_abnormal


def get_dashboard_title() -> None:
    """Returns a title for the dashboard."""
    st.title("Deloton Bike Analysis")


def get_current_ride_header(rider_name: str) -> None:
    """Returns a header for the current ride and the rider's name."""
    st.header(f"CURRENT RIDE: {rider_name}", divider='blue')


def get_last_updated_current_ride(last_update_time: datetime,
                                  last_updated_placeholder: st.empty) -> None:
    """Returns a caption under the header with the time since the last data update."""

    # time_delta = (current_time-last_update_time).total_seconds()

    current_time = datetime.utcnow()
    time_delta = int((current_time - last_update_time).total_seconds())

    last_updated_placeholder.caption(
        f"Last updated: {time_delta} seconds ago")


def get_current_ride_header_personal_info(user_details) -> None:
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
            f'''WARNING! HEART RATE IS ZERO: \n
            PLEASE PLACE HANDS ON HANDLE BARS OR SEEK ASSISTANCE!''', icon="⚠️")
    else:
        st.warning(
            f'''WARNING! HEART RATE IS OUTSIDE THE HEALTHY RANGE: \n
            PLEASE SLOW DOWN OR SEEK ASSISTANCE!''', icon="⚠️")


def get_current_ride_metrics(user_details) -> None:
    """
    Gets the header metrics for the current ride and displays them.
    """

    # get metrics
    heart_rate = user_details[7]
    power = round(user_details[8], 1)
    resistance = user_details[9]
    elapsed_time = user_details[10]

    if is_heart_rate_abnormal(user_details):
        get_heart_rate_warning(heart_rate)

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


def get_current_ride_personal_best_metrics(user_best_details) -> None:
    """
    Gets the main header metric personal bests for the current ride and displays them.
    """
    with st.expander('Personal Best ⛛'):
        get_current_ride_metrics(user_best_details)
