"""Utility functions to transform data fetched from SQL for use in visualisations."""

from datetime import datetime, timedelta

import pandas as pd


def get_current_rider_name(current_ride: list) -> str:
    """Returns a string containing the rider first and last name."""
    first_name = current_ride[1]
    last_name = current_ride[2]
    rider_name = f"{first_name} {last_name}"
    return rider_name


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


def calculate_max_heart_rate(user_details: list) -> int:
    """
    Returns the maximum heart rate for the given user based on their age and gender.

    'birthdate' and 'gender' are assumed to be cleaned and
    always as datetime and str types, respectively.

    'other' and 'None' gender heart rates are treated conservatively using the formula
    for females as a safety precaution.
    """
    birthdate = user_details[6]
    age = calculate_age(birthdate)
    gender = user_details[5]

    if gender in ("female", "other", None):
        return round(206 - (0.88 * age))
    if gender == "male" and age < 40:
        return round(220 - age)

    return round(208 - (0.7 * age))


def calculate_min_heart_rate(user_details: list) -> int:
    """
    Returns the minimum heart rate for the given user based on their age and gender.

    'birthdate' and 'gender' are assumed to be cleaned and
    always as datetime and str types, respectively.

    'other' and 'None' gender heart rates are treated conservatively using the formula
    for females as a safety precaution.
    """
    birthdate = user_details[6]
    age = calculate_age(birthdate)
    gender = user_details[5]

    if gender in ("female", "other", None):

        if 18 <= age <= 39:
            return 45
        if 40 <= age <= 64:
            return 52
        return 57

    # male
    if 18 <= age <= 39:
        return 40
    if 40 <= age <= 64:
        return 47

    return 52


def is_heart_rate_abnormal(user_details: list) -> bool:
    """Returns True for heart rate outside of the safe range."""

    min_heart_rate = calculate_min_heart_rate(user_details)
    max_heart_rate = calculate_max_heart_rate(user_details)
    heart_rate = user_details[7]

    return (heart_rate == 0) or not (min_heart_rate <= heart_rate <= max_heart_rate)


def process_dataframe_types(recent_rides: pd.DataFrame) -> pd.DataFrame:
    """Modifies by reference the given DataFrame column types"""
    recent_rides['elapsed_time'] = pd.to_numeric(recent_rides['elapsed_time'])

    return recent_rides


def get_dataframe_columns_for_line_charts(recent_rides: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the reading_time from the start_time and elapsed_time columns,
    and returns the resulting DataFrame containing only relevant columns
    to speed up aggregation of data.
    """

    df = recent_rides[['elapsed_time',
                       'start_time', 'power', 'resistance']].copy()

    df["reading_time"] = df.apply(
        lambda x: (x['start_time'] + timedelta(seconds=x['elapsed_time'])).round('min'), axis=1)

    print(df.sort_values('reading_time', ascending=True))
    return df


def process_dataframe_power_output_avg(ride_data: pd.DataFrame) -> pd.DataFrame:
    """Returns a DataFrame that average power output per minute."""

    grouped_df = ride_data.groupby(
        ['reading_time'])['power'].mean().astype(int)

    grouped_df = grouped_df.reset_index()

    grouped_df.columns = ['reading_time', 'power']

    return grouped_df


def process_dataframe_resistance_output_avg(ride_data: pd.DataFrame) -> pd.DataFrame:
    """Returns a DataFrame that average resistance per minute."""

    grouped_df = ride_data.groupby(
        ['reading_time'])['resistance'].mean().astype(int)

    grouped_df = grouped_df.reset_index()

    grouped_df.columns = ['reading_time', 'resistance']

    return grouped_df


def process_dataframe_power_output_cumul(ride_data: pd.DataFrame) -> pd.DataFrame:
    """Returns a DataFrame that cumulative power output per minute."""

    grouped_df = ride_data.groupby(
        ['reading_time'])['power'].cumsum().astype(int)

    grouped_df = grouped_df.reset_index()

    grouped_df.columns = ['reading_time', 'power']

    return grouped_df


def process_dataframe_resistance_output_cumul(ride_data: pd.DataFrame) -> pd.DataFrame:
    """Returns a DataFrame that cumulative resistance per minute."""

    grouped_df = ride_data.groupby(
        ['reading_time'])['resistance'].cumsum().astype(int)

    grouped_df = grouped_df.reset_index()

    grouped_df.columns = ['reading_time', 'resistance']

    return grouped_df
