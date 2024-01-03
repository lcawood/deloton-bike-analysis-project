# pylint: skip-file
from datetime import datetime

from transform import timestamp_to_date, check_datetime_is_valid, extract_datetime_from_string, \
    get_bike_serial_number_from_log_line, get_email_from_log_line, get_user_from_log_line, \
    get_ride_data_from_log_line, get_reading_data_from_log_line, get_address_from_log_line


def test_convert_epoch_to_datetime():
    assert timestamp_to_date(0) == '1970-01-01'

    assert timestamp_to_date(1609459200000) == '2021-01-01'

    assert timestamp_to_date(1612137600000) == '2021-02-01'

    assert timestamp_to_date(1614556800000) == '2021-03-01'

    # Test with current time
    current_time = int(datetime.utcnow().timestamp() * 1000)
    assert timestamp_to_date(
        current_time) == datetime.now().strftime('%Y-%m-%d')

    assert timestamp_to_date(-631152000000) == '1950-01-01'


def test_valid_datetime_object():
    assert check_datetime_is_valid(
        datetime(2024, 1, 1, 1, 2, 3, 123456)) == True
    assert check_datetime_is_valid(
        datetime(2023, 12, 30, 1, 2, 3, 123456)) == True


def test_invalid_datetime_object():
    assert check_datetime_is_valid(
        datetime(2025, 1, 1, 1, 2, 3, 123456)) == False
    assert check_datetime_is_valid(
        datetime(1899, 12, 30, 1, 2, 3, 123456)) == False


def test_valid_date_in_log_line():
    assert extract_datetime_from_string(
        'some text 2024-01-01 01:02:03.123456 some text') == datetime(2024, 1, 1, 1, 2, 3, 123456)


def test_no_date_found_in_log_line():
    assert extract_datetime_from_string(
        'some text some text some text') == None


def test_future_date_found_in_log_line():
    assert extract_datetime_from_string(
        'some text 2025-01-01 01:02:03.123456 some text') == None


def test_invalid_date_found_in_log_line():
    assert extract_datetime_from_string(
        'some text 2024-01-01 50:02:03.123456 some text') == None

    assert extract_datetime_from_string(
        'some text 1850-01-01 50:02:03.123456 some text') == None


def test_get_serial_number_from_log_line():
    assert get_bike_serial_number_from_log_line(
        "2022-07-25 16:13:37.209120 mendoza v9: [SYSTEM] data = \
        {\"user_id\":815,\"name\":\"Wayne Fitzgerald\",\"gender\":\"male\",\
        \"address\":\"Studio 3,William alley,New Bethan,WR4V 7TA\",\
        \"date_of_birth\":-336700800000,\"email_address\":\"wayne_fitzgerald@hotmail.com\",\
        \"height_cm\":187,\"weight_kg\":52,\"account_create_date\":1641254400000,\
        \"bike_serial\":\"SN0000\",\"original_source\":\"offline\"}\n"
    ) == "SN0000"


def test_no_serial_number_contained_in_log_line():
    assert get_bike_serial_number_from_log_line(
        "2022-07-25 16:13:37.209120 mendoza v9: [SYSTEM] data = \
        {\"user_id\":815,\"name\":\"Wayne Fitzgerald\",\"gender\":\"male\",\
        \"address\":\"Studio 3,William alley,New Bethan,WR4V 7TA\",\
        \"date_of_birth\":-336700800000,\"email_address\":\"wayne_fitzgerald@hotmail.com\",\
        \"height_cm\":187,\"weight_kg\":52,\"account_create_date\":1641254400000,\
        \"original_source\":\"offline\"}\n"
    ) == None


def test_valid_email_in_log_line():
    assert get_email_from_log_line(
        "2022-07-25 16:13:37.209120 mendoza v9: [SYSTEM] data = \
    {\"user_id\":815,\"name\":\"Wayne Fitzgerald\",\"gender\":\"male\",\
    \"address\":\"Studio 3,William alley,New Bethan,WR4V 7TA\",\
    \"date_of_birth\":-336700800000,\"email_address\":\"wayne_fitzgerald@hotmail.com\",\
    \"height_cm\":187,\"weight_kg\":52,\"account_create_date\":1641254400000,\
    \"original_source\":\"offline\"}\n"
    ) == 'wayne_fitzgerald@hotmail.com'


def test_no_email_found_in_log_line():
    assert get_email_from_log_line(
        "2022-07-25 16:13:37.209120 mendoza v9: [SYSTEM] data = \
    {\"user_id\":815,\"name\":\"Wayne Fitzgerald\",\"gender\":\"male\",\
    \"address\":\"Studio 3,William alley,New Bethan,WR4V 7TA\",\
    \"date_of_birth\":-336700800000,\
    \"height_cm\":187,\"weight_kg\":52,\"account_create_date\":1641254400000,\
    \"original_source\":\"offline\"}\n"
    ) == None


def test_get_valid_user_data_from_log_line():
    assert get_user_from_log_line(
        "2022-07-25 16:13:37.209120 mendoza v9: [SYSTEM] data = \
        {\"user_id\":815,\"name\":\"Wayne Fitzgerald\",\"gender\":\"male\",\
        \"address\":\"Studio 3,William alley,New Bethan,WR4V 7TA\",\
        \"date_of_birth\":-336700800000,\"email_address\":\"wayne_fitzgerald@hotmail.com\",\
        \"height_cm\":187,\"weight_kg\":52,\"account_create_date\":1641254400000,\
        \"bike_serial\":\"SN0000\",\"original_source\":\"offline\"}\n"
    ) == {'user_id': 815, 'first_name': 'Wayne', 'last_name': 'Fitzgerald',
          'birthdate': '1959-05-02', 'height': 187, 'weight': 52,
          'email': 'wayne_fitzgerald@hotmail.com', 'gender': 'male', 'account_created': '2022-01-04'}


def test_valid_ride_data_from_log_line():
    assert get_ride_data_from_log_line(
        "2022-07-25 16:13:37.209120 mendoza v9: [SYSTEM] data = \
        {\"user_id\":815,\"name\":\"Wayne Fitzgerald\",\"gender\":\"male\",\
        \"address\":\"Studio 3,William alley,New Bethan,WR4V 7TA\",\
        \"date_of_birth\":-336700800000,\"email_address\":\"wayne_fitzgerald@hotmail.com\",\
        \"height_cm\":187,\"weight_kg\":52,\"account_create_date\":1641254400000,\
        \"bike_serial\":\"SN0000\",\"original_source\":\"offline\"}\n"
    ) == {'user_id': 815, 'start_time': '2022-07-25 16:13:36'}


def test_invalid_ride_data_from_log_line():
    assert get_ride_data_from_log_line(
        "mendoza v9: [SYSTEM] data = \
        {\"name\":\"Wayne Fitzgerald\",\"gender\":\"male\",\
        \"address\":\"Studio 3,William alley,New Bethan,WR4V 7TA\",\
        \"date_of_birth\":-336700800000,\"email_address\":\"wayne_fitzgerald@hotmail.com\",\
        \"height_cm\":187,\"weight_kg\":52,\"account_create_date\":1641254400000,\
        \"bike_serial\":\"SN0000\",\"original_source\":\"offline\"}\n"
    ) == {'user_id': None, 'start_time': None}


def test_valid_reading_data_from_log_line():
    assert get_reading_data_from_log_line({}, "2022-07-25 16:13:37.709125 mendoza v9: \
                                          [INFO]: Ride - duration = 1.0; resistance = 30\n",
                                          datetime(2022, 7, 25, 16,
                                                   13, 30, 709125)
                                          ) == {'resistance': 30, 'elapsed_time': 7}

    assert get_reading_data_from_log_line({}, "2022-07-25 16:13:41.209157 mendoza v9: [INFO]:\
                                           Telemetry - hrt = 84; rpm = 27; power = 5.092422057\n",
                                          datetime(2022, 7, 25, 16,
                                                   13, 30, 709125)
                                          ) == {'heart_rate': 84, 'rpm': 27, 'power': 5.092422057}


def test_invalid_reading_data_from_log_line():
    assert get_reading_data_from_log_line({}, "2022-07-25 16:13:37.709125 mendoza v9: \
                                          [INFO]: Ride - duration = 1.0;",
                                          datetime(2022, 7, 25, 16,
                                                   13, 30, 709125)
                                          ) == {'resistance': None, 'elapsed_time': 7}

    assert get_reading_data_from_log_line({}, "mendoza v9: \
                                          [INFO]: Ride - duration = 1.0; resistance = 30\n",
                                          datetime(2022, 7, 25, 16,
                                                   13, 30, 709125)
                                          ) == {'resistance': 30, 'elapsed_time': None}


def test_start_time_after_reading_time_in_log_line():
    assert get_reading_data_from_log_line({}, "2022-07-25 16:13:37.709125 mendoza v9: \
                                          [INFO]: Ride - duration = 1.0; resistance = 30\n",
                                          datetime(2100, 7, 25, 16,
                                                   13, 30, 709125)
                                          ) == {'resistance': 30, 'elapsed_time': None}


def test_get_valid_address_from_log_line():
    assert get_address_from_log_line(
        "2022-07-25 16:13:37.209120 mendoza v9: [SYSTEM] data = \
        {\"user_id\":815,\"name\":\"Wayne Fitzgerald\",\"gender\":\"male\",\
        \"address\":\"10 Example Street, Example Area, London, AB1 2CD\",\
        \"date_of_birth\":-336700800000,\"email_address\":\"wayne_fitzgerald@hotmail.com\",\
        \"height_cm\":187,\"weight_kg\":52,\"account_create_date\":1641254400000,\
        \"bike_serial\":\"SN0000\",\"original_source\":\"offline\"}\n"
    ) == {'first_line': '10 Example Street', 'second_line': 'Example Area',
          'city': 'London', 'postcode': 'AB1 2CD'}


def test_missing_address_from_log_line():
    assert get_address_from_log_line(
        "2022-07-25 16:13:37.209120 mendoza v9: [SYSTEM] data = \
        {\"user_id\":815,\"name\":\"Wayne Fitzgerald\",\"gender\":\"male\",\
        \"date_of_birth\":-336700800000,\"email_address\":\"wayne_fitzgerald@hotmail.com\",\
        \"height_cm\":187,\"weight_kg\":52,\"account_create_date\":1641254400000,\
        \"bike_serial\":\"SN0000\",\"original_source\":\"offline\"}\n"
    ) == {'first_line': None, 'second_line': None,
          'city': None, 'postcode': None}
