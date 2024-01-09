"""
Entry point home page for the dashboard.
"""

# 'Unable to import' errors
# pylint: disable = E0401


from dotenv import load_dotenv
import streamlit as st

from database import (get_database_connection,
                      get_total_ride_count, get_max_readings)


def main_homepage():
    """Generates the visualisations for the dashboard home page."""
    st.set_page_config(
        page_title="DELOTON Dashboard",
        page_icon="🚲",
        layout="wide"
    )

    st.write("# DELOTON Bike Analysis🚴")

    st.sidebar.success("Select a page above.")

    st.markdown(
        """
        Realtime dashboard to give the business visibility on the
        current and recent behaviour of riders.\n

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
    head_cols = st.columns(4)
    with head_cols[0]:
        st.metric("Total Rides", total_ride_count)
    with head_cols[1]:
        st.metric("Max Elapsed Time", f"{max_elapsed_time} secs")
    with head_cols[2]:
        st.metric("Power", f"{round(max_power, 1)} W")
    with head_cols[3]:
        st.metric("Resistance", max_resistance)
