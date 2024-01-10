"""Functions for visualising Deloton Bike ride data on the Streamlit app."""

# 'Unable to import' errors
# pylint: disable = E0401

from datetime import datetime

import altair as alt
import pandas as pd
import streamlit as st

from utilities import calculate_age, get_gender_emoji

# ------------------ HOME ----------------------


def get_summary_statistics(total_ride_count: int, max_elapsed_time: int,
                           max_power: float, max_resistance: int) -> None:
    """Generates a header with historical summary statistics."""

    head_cols = st.columns(4)
    with head_cols[0]:
        st.metric("Total Rides", total_ride_count)
    with head_cols[1]:
        st.metric("Max Elapsed Time", f"{max_elapsed_time} secs")
    with head_cols[2]:
        st.metric("Max Power", f"{round(max_power, 1)} W")
    with head_cols[3]:
        st.metric("Max Resistance", max_resistance)


# -------------- CURRENT RIDE -----------------
def get_current_ride_header(rider_name: str) -> None:
    """Generates a header for the current ride and the rider's name."""
    st.title(f"CURRENT RIDE: {rider_name}")
    st.markdown('<hr style="border: 1px solid green; margin-top: 0em; margin-bottom: 0.5em;">',
                unsafe_allow_html=True)


def get_current_rider_info_header() -> None:
    """Generates a header for the current rider's personal info and personal bests."""
    st.header("RIDER INFO", divider='green')


def get_personal_info_subheader() -> None:
    """Generates a subheader for the current rider's personal info."""
    for i in range(2):
        st.markdown(" ")
    st.subheader("Personal Info", divider='green')


def get_personal_best_subheader() -> None:
    """Generates a subheader for the current rider's personal bests."""
    for i in range(2):
        st.markdown(" ")
    st.subheader("Personal Best", divider='green')


def get_last_updated_current_ride(last_update_time: datetime,
                                  _last_updated_placeholder: st.empty) -> None:
    """Generates a caption under the header with the time since the last data update."""

    current_time = datetime.utcnow()
    time_delta = int((current_time - last_update_time).total_seconds())

    _last_updated_placeholder.caption(
        f"Last updated: {time_delta} seconds ago")


def get_current_ride_header_personal_info(user_details: list) -> None:
    """
    Gets the main header personal_info for the current ride and displays them.
    """

    # get metrics
    height = user_details[3]
    weight = user_details[4]
    gender = user_details[5]

    gender_emoji = get_gender_emoji(gender)

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


def get_current_ride_personal_best_metrics(user_best_details: list) -> None:
    """
    Generates the main header metric personal bests for the current ride and displays them.
    """
    with st.container():
        get_current_ride_metrics(user_best_details)


# -------------- RECENT RIDES -----------------
def get_recent_rides_header() -> None:
    """Generates a header for the recent rides section."""
    st.title("RECENT RIDES")
    st.markdown('<hr style="border: 1px solid green; margin-top: 0em; margin-bottom: 0.5em;">',
                unsafe_allow_html=True)


def get_last_updated_recent_rides(last_update_time: datetime,
                                  _last_updated_placeholder: st.empty) -> None:
    """Generates a caption under the header with the time since the last data update."""

    current_time = datetime.utcnow()
    time_delta = int((current_time - last_update_time).total_seconds())

    _last_updated_placeholder.caption(
        f"Last updated: {time_delta} seconds ago")


def get_total_duration_gender_bar_chart(recent_data: pd.DataFrame,
                                        selector_gender, selector_age) -> alt.Chart:
    """
    Generates a bar chart for the total elapsed_time grouped by gender
    over the past 12 hours.
    """

    # chart_width = 220
    # dx_offset = 100

    chart = alt.Chart(recent_data).add_selection(selector_gender).transform_filter(
        selector_gender & selector_age).transform_aggregate(
        max_elapsed_time='max(elapsed_time)',
        groupby=['ride_id', 'Gender']
    ).transform_aggregate(
        total_elapsed_time='sum(max_elapsed_time)',
        groupby=['Gender']
    ).transform_calculate(
        total_elapsed_time_minutes='datum.total_elapsed_time / 60'
    ).mark_bar().encode(
        y=alt.X('Gender:N', title='Gender'),
        x=alt.Y('total_elapsed_time_minutes:Q',
                title='Total Elapsed Time (minutes)'),
        tooltip=[alt.Tooltip('Gender:N', title='Gender'), alt.Tooltip(
            'total_elapsed_time_minutes:Q', title='Total Elapsed Time', format=".1f")],
        opacity=alt.condition(selector_gender, alt.value(1), alt.value(0.25))
    ).properties(
        title={'text': 'Total Duration', 'fontSize': 20})

    return chart


def get_total_ride_count_gender_bar_chart(recent_rides: pd.DataFrame,
                                          selector_gender, selector_age) -> alt.Chart:
    """
    Generates a bar chart for the total number of rides grouped by gender
    over the past 12 hours.
    """

    # chart_width = 220
    # dx_offset = 60

    chart = alt.Chart(recent_rides).mark_bar().encode(
        y=alt.X('Gender:N', title='Gender'),
        x=alt.Y('distinct(ride_id):Q', title='Number of Rides'),
        opacity=alt.condition(selector_gender, alt.value(1), alt.value(0.25))
    ).add_selection(selector_gender).transform_filter(
        selector_gender & selector_age).properties(
        title={'text': 'Total Rides (by gender)', 'fontSize': 20})

    return chart


def get_total_ride_count_age_bar_chart(ride_counts: pd.DataFrame,
                                       selector_gender, selector_age) -> alt.Chart:
    """
    Generates a bar chart for the total number of rides grouped by age brackets
    over the past 12 hours.
    """

    # chart_width = 540
    # dx_offset = 225

    # chart = alt.Chart(ride_counts).mark_bar().encode(
    #     y=alt.X('Age Bracket:N', title='Ages'),
    #     x=alt.Y('distinct(ride_id):Q', title='Number of Rides'),
    #     tooltip=[alt.Tooltip('Age Bracket:N', title='Age Bracket'), alt.Tooltip(
    #         'count():Q', title='Total Number of Rides')],
    # ).add_selection(selector_age).transform_filter(
    #     selector_gender & selector_age).properties(
    #     title={'text': 'Total Rides (by age)', 'fontSize': 20},
    #     autosize='fit')

    pie_chart = alt.Chart(ride_counts).mark_arc(radius=150).encode(
        color=alt.Color('Age Bracket:N', title='Age Brackets'),
        theta=alt.Theta('distinct(ride_id):Q',
                        title='Number of Rides', stack=True),
        tooltip=[alt.Tooltip('Age Bracket:N', title='Age Bracket'), alt.Tooltip(
            'count():Q', title='Total Number of Rides')],
    ).add_selection(selector_age).transform_filter(
        selector_gender & selector_age).properties(
        title={'text': 'Total Rides (by Age Bracket)', 'fontSize': 20})

#
    # title={'text': 'Total Rides (by Age Bracket)', 'fontSize': 20, })

    text = pie_chart.mark_text(
        align="center",
        outerRadius=175,
        fontSize=14).encode(
        text='Age Bracket:N',
        theta=alt.Theta('distinct(ride_id):Q',
                        title='Number of rides', stack=True)
    )

    chart = alt.layer(pie_chart, text)

    return chart


def get_power_output_avg_line_chart(recent_data: pd.DataFrame,
                                    selector_gender, selector_age) -> alt.Chart:
    """Generates a line chart for the average power output over the past 12 hours."""

    chart_width = 600
    dx_offset = 250

    chart = alt.Chart(
        recent_data
    ).mark_line(
        interpolate='linear'
    ).transform_filter(
        selector_gender & selector_age
    ).encode(
        x=alt.X('reading_time:T', axis=alt.Axis(title='Time', grid=False)),
        y=alt.Y('mean(power):Q', title='Average Power (W)'),
        tooltip=[alt.Tooltip('reading_time:N', title='Reading Time'), alt.Tooltip(
            'mean(power):Q', title='Average Power')]
    ).properties(
        title={'text': 'Average Power Output', 'fontSize': 20})

    return chart


def get_resistance_output_avg_line_chart(recent_data: pd.DataFrame,
                                         selector_gender, selector_age) -> alt.Chart:
    """Generates a line chart for the average resistance output over the past 12 hours."""

    chart_width = 600
    dx_offset = 250

    chart = alt.Chart(
        recent_data
    ).mark_line(
        interpolate='linear'
    ).transform_filter(
        selector_gender & selector_age
    ).encode(
        x=alt.X('reading_time:T', axis=alt.Axis(title='Time')),
        y=alt.Y('mean(resistance):Q', title='Average Resistance'),
    ).properties(
        title={'text': 'Average Resistance Output', 'fontSize': 20})

    return chart


def get_power_output_cumul_line_chart(recent_data: pd.DataFrame,
                                      selector_gender, selector_age) -> alt.Chart:
    """Generates a line chart for the cumulative power output over the past 12 hours."""

    # chart_width = 600
    # dx_offset = 250

    recent_data['kilowatt_power'] = recent_data['power']/1000

    chart = alt.Chart(recent_data).mark_line().transform_filter(
        selector_gender & selector_age
    ).encode(
        x=alt.X('reading_time:T', axis=alt.Axis(title='Time')),
        y=alt.Y('cumulative_power:Q', title='Cumulative Power (kW)'),
    ).transform_window(
        cumulative_power='sum(kilowatt_power)',
        sort=[{"field": 'reading_time'}]
    ).properties(
        title={'text': 'Cumulative Power Output', 'fontSize': 20})

    return chart


def get_resistance_output_cumul_line_chart(recent_data: pd.DataFrame,
                                           selector_gender, selector_age) -> alt.Chart:
    """Generates a line chart for the cumulative resistance output over the past 12 hours."""

    chart_width = 600
    dx_offset = 250

    chart = alt.Chart(recent_data).mark_line().transform_filter(
        selector_gender & selector_age
    ).encode(
        x=alt.X('reading_time:T', axis=alt.Axis(title='Time')),
        y=alt.Y('cumulative_resistance:Q', title='Cumulative Resistance'),
    ).transform_window(
        cumulative_resistance='sum(resistance)',
        sort=[{"field": 'reading_time'}]
    ).properties(
        title={'text': 'Cumulative Resistance Output', 'fontSize': 20})

    return chart


def get_sidebar_age_filter(recent_data: pd.DataFrame) -> None:
    """Generates a sidebar filter for filtering by gender."""

    filtered_data = recent_data.copy()

    genders = filtered_data['gender'].unique()
    with st.sidebar:
        genders_selected = st.multiselect(
            'Select genders', genders, default=genders)

    # mask to filter DataFrame
    mask_genders = filtered_data['gender'].isin(genders_selected)

    filtered_data = filtered_data[mask_genders]

    return filtered_data
