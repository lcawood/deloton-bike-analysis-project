"""Unit tests for the report.py file"""
from unittest.mock import MagicMock,patch
from datetime import date, datetime
from freezegun import freeze_time

import pandas as pd

from report import create_gender_split_table,create_user_stats_table,create_html_string, previous_day_from_database,sql_select_all_useful_data,convert_to_age,extract_report_data,create_report_data,handler

EXAMPLE_GENDER_DICT = [{'gender': 'female', 'count': 16}, {'gender': 'male', 'count': 13}]
EXAMPLE_AGE_DICT = [{'rider_id': 712, 'Age': 70}, {'rider_id': 713, 'Age': 62}]
EXAMPLE_POWER_DICT = [{'rider_id': 712, 'average_power': 43.57344918850486}, 
                      {'rider_id': 713, 'average_power': 62.949498282492094}]
EXAMPLE_HEART_RATE_DICT = [{'rider_id': 712, 'avg': 92.47115384615384}, 
                           {'rider_id': 713, 'avg': 115.65092655699591}]

EXAMPLE_RIDE_DICT = {'power': [{'rider_id': 795, 'average_power': 55.93454529937276}, 
                               {'rider_id': 796, 'average_power': 40.9406303}], 
                     'heart_rate': [{'rider_id': 795, 'avg': 111.02109854967553}, 
                                    {'rider_id': 796, 'avg': 91.2131693284919}], 
                     'amount_of_rides': 170, 
                     'gender': [{'gender': 'female', 'count': 11}, {'gender': 'male', 'count': 15}], 
                     'ages': [{'rider_id': 795, 'Age': 60}, {'rider_id': 796, 'Age': 36}]}


EXAMPLE_SQL_FETCHED_DICT = [{'ride_id': 5609, 'rider_id': 1345, 'bike_id': 1, 
                             'start_time': pd.Timestamp('2023-11-08 00:04:15.434890'), 
                             'gender': 'male', 'birthdate': date(1974, 3, 21), 
                             'avg': 122.62088974854932, 'average_power': 56.05435014914318}, 
                             {'ride_id': 5610, 'rider_id': 1345, 'bike_id': 1, 
                              'start_time': pd.Timestamp('2023-11-08 00:12:54.447122'), 
                              'gender': 'male', 'birthdate': date(1974, 3, 21), 
                              'avg': 116.86872586872587, 'average_power': 64.37955624535546}]

EXAMPLE_DATAFRAME = pd.DataFrame(EXAMPLE_SQL_FETCHED_DICT)

def test_create_gender_split_table():
    """Tests table creation for the gender split works correctly."""

    html_string_output = create_gender_split_table(EXAMPLE_GENDER_DICT)

    assert "male" in html_string_output
    assert "16" in html_string_output
    assert "<tr>" in html_string_output
    assert "</tr>" in html_string_output

def test_test_create_gender_split_table_works_empty_list():
    """Tests table creation for the gender split works even if gender_dict is empty."""

    html_string_output = create_gender_split_table([])

    assert "male" in html_string_output
    assert "0" in html_string_output
    assert "<tr>" in html_string_output
    assert "</tr>" in html_string_output


def test_create_user_stats_table():
    """Tests the user statistics table creates the correct details and functions"""

    html_string_output = create_user_stats_table(EXAMPLE_AGE_DICT,EXAMPLE_POWER_DICT,EXAMPLE_HEART_RATE_DICT)

    assert "43.57344918850486" in html_string_output
    assert "712" in html_string_output
    assert "70" in html_string_output
    assert "62.949498282492094" in html_string_output
    assert "<tr>" in html_string_output
    assert "</tr>" in html_string_output

def test_create_html_string_is_correct():
    """Tests the html string created contains the correct info"""

    html_output_string = create_html_string(EXAMPLE_RIDE_DICT,"2024-10-05")

    assert "<head>" in html_output_string
    assert "</body>" in html_output_string
    assert "2024-10-05" in html_output_string
    assert "795" in html_output_string


def test_previous_day_from_database_current():
    """
    Test the get previous day function correctly 
    calculates yesterday from a present date
    """

    mock_conn = MagicMock()

    mock_execute = mock_conn.cursor.return_value\
            .__enter__.return_value\
            .execute
    mock_fetch = mock_conn.cursor.return_value\
            .__enter__.return_value\
            .fetchone
    
    datetime_object = datetime.strptime("2024-01-04", '%Y-%m-%d')

    mock_fetch.return_value = (datetime_object,)

    assert previous_day_from_database(mock_conn) == "2024-01-03"

def test_previous_day_from_database_on_year():
    """
    Test the get previous day function correctly 
    calculates yesterday if it currently the first of a year.
    """

    mock_conn = MagicMock()

    mock_execute = mock_conn.cursor.return_value\
            .__enter__.return_value\
            .execute
    mock_fetch = mock_conn.cursor.return_value\
            .__enter__.return_value\
            .fetchone
    
    datetime_object = datetime.strptime("2024-01-01", '%Y-%m-%d')

    mock_fetch.return_value = (datetime_object,)

    assert previous_day_from_database(mock_conn) == "2023-12-31"

@patch('report.pd.read_sql_query')
def test_sql_select_all_useful_data(mock_read_sql_query):
    """Tests the function to select data from the data warehouse functions"""

    mock_conn = MagicMock()

    mock_read_sql_query.return_value = pd.DataFrame(EXAMPLE_GENDER_DICT)

    assert isinstance(sql_select_all_useful_data(mock_conn),pd.DataFrame)


@freeze_time("2000-01-04")
def test_convert_to_age():
    """Tests the function to convert a date to a age returns the correct value"""

    assert convert_to_age("1997-04-13") == 2
    assert convert_to_age("1943-04-13") == 56


def test_extract_report_data():
    """Tests the extracting report data returns a dict with the correct keys"""

    rides_dict = extract_report_data(EXAMPLE_DATAFRAME)

    assert "power" in rides_dict
    assert "ages" in rides_dict
    assert rides_dict["power"][0]["rider_id"] == 1345
    assert rides_dict["amount_of_rides"] == 2

@patch('report.sql_select_all_useful_data')
@patch('report.extract_report_data')
def test_create_report_data(mock_extract_report_data,mock_sql_select_all_useful_data):
    """Tests a body of html gets returned from the function"""

    mock_conn = MagicMock()

    yesterday = datetime.strptime("2024-01-03", '%Y-%m-%d')

    mock_extract_report_data.return_value = EXAMPLE_RIDE_DICT

    html_body = create_report_data(yesterday,mock_conn)

    assert isinstance(html_body, dict)
    assert isinstance(html_body["html_body"],str)
    assert "</body>" in html_body["html_body"]

@patch('report.previous_day_from_database')
@patch('report.get_s3_client')
@patch('report.get_database_connection')
@patch('report.load_dotenv')
@patch('report.sql_select_all_useful_data')
@patch('report.extract_report_data')
def test_handler_returns_body(mock_extract_report_data,mock_sql_select_all_useful_data,
                              mock_load_dotenv,mock_get_database_connection,
                              mock_get_s3_client,mock_previous_day_from_database):
    """Tests the handler function correctly returns the html body"""

    mock_extract_report_data.return_value = EXAMPLE_RIDE_DICT

    mock_get_database_connection.return_value = MagicMock()

    handler_return = handler(0,0)

    assert handler_return["statusCode"] == 200
    assert "<head><style>" in handler_return["body"]


@patch('report.previous_day_from_database')
@patch('report.get_s3_client')
@patch('report.get_database_connection')
@patch('report.load_dotenv')
@patch('report.sql_select_all_useful_data')
@patch('report.extract_report_data')
def test_handler_returns_error_message(mock_extract_report_data,mock_sql_select_all_useful_data,
                              mock_load_dotenv,mock_get_database_connection,
                              mock_get_s3_client,mock_previous_day_from_database):
    """Tests the handler function correctly returns the html body"""

    mock_extract_report_data.return_value = ValueError

    mock_get_database_connection.return_value = MagicMock()

    handler_return = handler(0,0)

    assert handler_return["statusCode"] == 404
    assert "ValueError" in handler_return["body"]