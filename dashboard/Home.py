"""
Entry point home page for the dashboard.
"""

# 'Unable to import' errors
# pylint: disable = E0401


from dotenv import load_dotenv
import streamlit as st

from database import (get_database_connection,
                      get_total_ride_count, get_max_readings)
from visualisations import get_summary_statistics


def main_homepage():
    """Generates the visualisations for the dashboard home page."""
    st.set_page_config(
        page_title="DELOTON Dashboard",
        page_icon="🚲",
        layout="wide"
    )

    st.write("# DELOTON Bike Analysis 🚴")

    st.sidebar.success("Select a page above.")

    st.markdown(
        """
        A dynamic, real-time dashboard that offers comprehensive statistics and visualisations,
        providing insights into the current and recent behavior of riders.\n

        **👈 Select a page from the sidebar** to see the current or recent rides.
        """)


if __name__ == "__main__":

    main_homepage()

    load_dotenv()

    conn = get_database_connection()

    total_ride_count = get_total_ride_count(conn)
    max_elapsed_time, max_power, max_resistance = get_max_readings(conn)

    st.markdown(" ")
    st.subheader("Summary Statistics:", divider='green')

    get_summary_statistics(
        total_ride_count, max_elapsed_time, max_power, max_resistance)
