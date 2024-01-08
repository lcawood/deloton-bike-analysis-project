"""
Entry point home page for the dashboard.
"""

# 'Unable to import' errors
# pylint: disable = E0401

import streamlit as st


def main_homepage():
    """Generates the visualisations for the dashboard home page."""
    st.set_page_config(
        page_title="DELOTON Dashboard",
        page_icon="🚲",
        layout="wide"
    )

    st.write("# DELOTON Bike Analysis🚴")

    st.markdown('''
    <style>
    .st-b7 {
        color: #90d1a2;
    }
    </style>
    ''', unsafe_allow_html=True)

    st.sidebar.success("Select a page above.")

    st.markdown(
        """
        Realtime dashboard to give the business visibility on the
        current and recent behaviour of riders.\n

        **👈 Select a page from the sidebar** to see the current or recent rides.
        """)


if __name__ == "__main__":

    main_homepage()
