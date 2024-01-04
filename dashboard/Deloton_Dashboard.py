import streamlit as st

st.set_page_config(
    page_title="Deloton Dashboard",
    page_icon="🚲",
)

st.write("# Deloton Bike Analysis Dashboard 🚲")

st.sidebar.success("Select a demo above.")

st.markdown(
    """
    Streamlit is an open-source app framework built specifically for
    Machine Learning and Data Science projects.
    **👈 Select a demo from the sidebar** to see some examples
    of what Streamlit can do!
    """)
