"""Test suite for the transform.py script, covering a range of possible errors in the
attained data from the log lines, as well as missing/incomplete data."""

from datetime import datetime, timedelta

from transform import timestamp_to_date, check_datetime_is_valid, extract_datetime_from_string, \
    get_bike_serial_number_from_log_line, get_email_from_log_line, get_user_from_log_line, \
    get_ride_data_from_log_line, get_reading_data_from_log_line, get_address_from_log_line


def test_convert_epoch_to_datetime():
    """Tests for the timestamp_to_date function where inputs are valid."""

    assert timestamp_to_date(0) == datetime(year=1970, month=1, day=1).date()

    assert timestamp_to_date(1609459200000) == datetime(year=2021, month=1, day=1).date()

    assert timestamp_to_date(1612137600000) == datetime(year=2021, month=2, day=1).date()

    assert timestamp_to_date(1614556800000) == datetime(year=2021, month=3, day=1).date()

    # Test with current time
    current_time = int(datetime.utcnow().timestamp() * 1000)
    assert timestamp_to_date(
        current_time) == datetime.now().date()

    assert timestamp_to_date(-631152000000) == datetime(year=1950, month=1, day=1).date()


def test_valid_datetime_object():
    """Tests for the check_datetime_is_valid function where the input is valid."""

    assert check_datetime_is_valid(
        datetime(2024, 1, 1, 1, 2, 3, 123456))
    assert check_datetime_is_valid(
        datetime(2023, 12, 30, 1, 2, 3, 123456))


def test_invalid_datetime_object():
    """Tests for the check_datetime_is_valid function where the input is invalid."""

    assert check_datetime_is_valid(
        datetime(2025, 1, 1, 1, 2, 3, 123456)) is False
    assert check_datetime_is_valid(
        datetime(1899, 12, 30, 1, 2, 3, 123456)) is False


def test_valid_date_in_log_line():
    """Test for the extract_datetime_from_string function where there is a valid input."""

    assert extract_datetime_from_string(
        'some text 2024-01-01 01:02:03.123456 some text') == datetime(2024, 1, 1, 1, 2, 3, 123456)


def test_no_date_found_in_log_line():
    """Test for the extract_datetime_from_string function where there is no date."""

    assert extract_datetime_from_string(
        'some text some text some text') is None


def test_future_date_found_in_log_line():
    """Test for the extract_datetime_from_string function where the date is in the future."""

    assert extract_datetime_from_string(
        'some text 2025-01-01 01:02:03.123456 some text') is None


def test_invalid_date_found_in_log_line():
    """Tests for the extract_datetime_from_string function where the dates are invalid."""

    assert extract_datetime_from_string(
        'some text 2024-01-01 50:02:03.123456 some text') is None

    assert extract_datetime_from_string(
        'some text 1850-01-01 50:02:03.123456 some text') is None


def test_get_serial_number_from_log_line():
    """Test for the get_bike_serial_number_from_log_line
    function where a serial number is present."""

    assert get_bike_serial_number_from_log_line(
        "2022-07-25 16:13:37.209120 mendoza v9: [SYSTEM] data = \
        {\"user_id\":815,\"name\":\"Wayne Fitzgerald\",\"gender\":\"male\",\
        \"address\":\"Studio 3,William alley,New Bethan,WR4V 7TA\",\
        \"date_of_birth\":-336700800000,\"email_address\":\"wayne_fitzgerald@hotmail.com\",\
        \"height_cm\":187,\"weight_kg\":52,\"account_create_date\":1641254400000,\
        \"bike_serial\":\"SN0000\",\"original_source\":\"offline\"}\n"
    ) == "SN0000"


def test_no_serial_number_contained_in_log_line():
    """Test for the get_bike_serial_number_from_log_line
    function where a serial number is not present."""

    assert get_bike_serial_number_from_log_line(
        "2022-07-25 16:13:37.209120 mendoza v9: [SYSTEM] data = \
        {\"user_id\":815,\"name\":\"Wayne Fitzgerald\",\"gender\":\"male\",\
        \"address\":\"Studio 3,William alley,New Bethan,WR4V 7TA\",\
        \"date_of_birth\":-336700800000,\"email_address\":\"wayne_fitzgerald@hotmail.com\",\
        \"height_cm\":187,\"weight_kg\":52,\"account_create_date\":1641254400000,\
        \"original_source\":\"offline\"}\n"
    ) is None


def test_valid_email_in_log_line():
    """Test for the get_email_from_log_line function where an email is present."""

    assert get_email_from_log_line(
        "2022-07-25 16:13:37.209120 mendoza v9: [SYSTEM] data = \
    {\"user_id\":815,\"name\":\"Wayne Fitzgerald\",\"gender\":\"male\",\
    \"address\":\"Studio 3,William alley,New Bethan,WR4V 7TA\",\
    \"date_of_birth\":-336700800000,\"email_address\":\"wayne_fitzgerald@hotmail.com\",\
    \"height_cm\":187,\"weight_kg\":52,\"account_create_date\":1641254400000,\
    \"original_source\":\"offline\"}\n"
    ) == 'wayne_fitzgerald@hotmail.com'


def test_no_email_found_in_log_line():
    """Test for the get_email_from_log_line function where an email is not present."""

    assert get_email_from_log_line(
        "2022-07-25 16:13:37.209120 mendoza v9: [SYSTEM] data = \
    {\"user_id\":815,\"name\":\"Wayne Fitzgerald\",\"gender\":\"male\",\
    \"address\":\"Studio 3,William alley,New Bethan,WR4V 7TA\",\
    \"date_of_birth\":-336700800000,\
    \"height_cm\":187,\"weight_kg\":52,\"account_create_date\":1641254400000,\
    \"original_source\":\"offline\"}\n"
    ) is None


def test_get_valid_user_data_from_log_line():
    """Test for the get_user_from_log_line function where all fields are present."""

    assert get_user_from_log_line(
        "2022-07-25 16:13:37.209120 mendoza v9: [SYSTEM] data = \
        {\"user_id\":815,\"name\":\"Wayne Fitzgerald\",\"gender\":\"male\",\
        \"address\":\"Studio 3,William alley,New Bethan,WR4V 7TA\",\
        \"date_of_birth\":-336700800000,\"email_address\":\"wayne_fitzgerald@hotmail.com\",\
        \"height_cm\":187,\"weight_kg\":52,\"account_create_date\":1641254400000,\
        \"bike_serial\":\"SN0000\",\"original_source\":\"offline\"}\n"
    ) == {'user_id': 815, 'first_name': 'Wayne', 'last_name': 'Fitzgerald',
          'birthdate': datetime(year=1959, month=5, day=2).date(), 'height': 187, 'weight': 52,
          'email': 'wayne_fitzgerald@hotmail.com', 'gender': 'male',
          'account_created': datetime(year=2022, month=1, day=4).date()}


def test_valid_ride_data_from_log_line():
    """Test for the get_ride_data_from_log_line function where all fields are present."""

    assert get_ride_data_from_log_line(
        "2022-07-25 16:13:37.209120 mendoza v9: [SYSTEM] data = \
        {\"user_id\":815,\"name\":\"Wayne Fitzgerald\",\"gender\":\"male\",\
        \"address\":\"Studio 3,William alley,New Bethan,WR4V 7TA\",\
        \"date_of_birth\":-336700800000,\"email_address\":\"wayne_fitzgerald@hotmail.com\",\
        \"height_cm\":187,\"weight_kg\":52,\"account_create_date\":1641254400000,\
        \"bike_serial\":\"SN0000\",\"original_source\":\"offline\"}\n"
    ) == {'user_id': 815, 'start_time': datetime.strptime('25/07/2022 16:13:37.209120', "%d/%m/%Y %H:%M:%S.%f") - timedelta(seconds=0.5)}


def test_invalid_ride_data_from_log_line():
    """Test for the get_ride_data_from_log_line function where all fields are not present."""

    assert get_ride_data_from_log_line(
        "mendoza v9: [SYSTEM] data = \
        {\"name\":\"Wayne Fitzgerald\",\"gender\":\"male\",\
        \"address\":\"Studio 3,William alley,New Bethan,WR4V 7TA\",\
        \"date_of_birth\":-336700800000,\"email_address\":\"wayne_fitzgerald@hotmail.com\",\
        \"height_cm\":187,\"weight_kg\":52,\"account_create_date\":1641254400000,\
        \"bike_serial\":\"SN0000\",\"original_source\":\"offline\"}\n"
    ) == {'user_id': None, 'start_time': None}


def test_valid_reading_data_from_log_line():
    """Tests for the get_reading_data_from_log_line function where all fields are present."""

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
    """Tests for the get_reading_data_from_log_line function where all fields are not present."""

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
    """Test for the get_reading_data_from_log_line function
    where the reading time is after the start time."""

    assert get_reading_data_from_log_line({}, "2022-07-25 16:13:37.709125 mendoza v9: \
                                          [INFO]: Ride - duration = 1.0; resistance = 30\n",
                                          datetime(2100, 7, 25, 16,
                                                   13, 30, 709125)
                                          ) == {'resistance': 30, 'elapsed_time': None}


def test_get_valid_address_from_log_line():
    """Test for the get_address_from_log_line function where an address is present."""

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
    """Test for the get_address_from_log_line function where an address is not present."""

    assert get_address_from_log_line(
        "2022-07-25 16:13:37.209120 mendoza v9: [SYSTEM] data = \
        {\"user_id\":815,\"name\":\"Wayne Fitzgerald\",\"gender\":\"male\",\
        \"date_of_birth\":-336700800000,\"email_address\":\"wayne_fitzgerald@hotmail.com\",\
        \"height_cm\":187,\"weight_kg\":52,\"account_create_date\":1641254400000,\
        \"bike_serial\":\"SN0000\",\"original_source\":\"offline\"}\n"
    ) == {'first_line': None, 'second_line': None,
          'city': None, 'postcode': None}
