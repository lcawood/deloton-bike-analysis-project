import streamlit as st

st.set_page_config(
    page_title="Deloton Dashboard",
    page_icon="🚲",
)

st.write("# Deloton Bike Analysis Dashboard 🚲")

st.sidebar.success("Select a demo above.")

st.markdown(
    """
    Realtime dashboard to give the business visibility on the
    current and recent behaviour of riders.
    **👈 Select a page from the sidebar** to see the current or recent rides.
    """)
