"""
Functions for visualising Deloton Bike ride data on the streamlit app.
"""

import pandas as pandas
import streamlit as st
import altair as alt


def get_current_ride_header(rider_name: str) -> None:
    """Returns a header for the current ride and the rider's name."""

    st.header(f"CURRENT RIDE: {rider_name}", divider='blue')
