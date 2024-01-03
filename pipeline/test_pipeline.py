"""Contains unit tests for pipeline.py"""

from unittest.mock import MagicMock, patch

from pipeline import get_next_log_line, user_pipeline, ride_pipeline, reading_pipeline, pipeline


def test_get_next_log_line():
    """Tests function get_next_log_line."""
    test_consumer = MagicMock()
    test_consumer.poll.side_effect = [
        {},
        None,
        {'not a log line': 42},
        {'log': 'this is a log line'},
        {'elves': 'magical creatures'}
    ]
    assert get_next_log_line(test_consumer) == 'this is a log line'


@patch('transform.get_user_from_log_line')
@patch('transform.get_address_from_log_line')
@patch('load.add_address')
@patch('load.add_user')
@patch('validate_heart_rate.calculate_max_heart_rate')
@patch('validate_heart_rate.calculate_min_heart_rate')
def test_user_pipeline(mock_min_heart_rate, mock_max_heart_rate, mock_load_user, mock_load_address,
                       mock_transform_address, mock_transform_user):
    """
    Tests user_pipeline function for patched transform, validate_heart_rate and load functions.
    """
    mock_transform_user.return_value = {'user_id': 7, 'forename': 'John', 'surname': 'Doe'}
    mock_transform_address.return_value = {'key': 'this is an address'}
    mock_load_address.return_value = 1
    mock_min_heart_rate.return_value = 60
    mock_max_heart_rate.return_value = 180

    user = user_pipeline('I am a log line')

    assert user == {
        'user_id': 7,
        'forename': 'John',
        'surname': 'Doe',
        'address_id': 1,
        'min_heart_rate': 60,
        'max_heart_rate': 180
        }


@patch('transform.get_ride_data_from_log_line')
@patch('load.add_ride')
def test_ride_pipeline(mock_load_ride, mock_transform_ride):
    """Tests function ride_pipeline for patched transform and load functions."""
    mock_transform_ride.return_value = {'key': 'value in a dictionary of ride info'}
    ride_pipeline('this is a log line', 2)
    mock_load_ride.assert_called_once_with(
        {'key': 'value in a dictionary of ride info', 'bike_id': 2})


@patch('pipeline.EXTREME_HR_COUNT_THRESHOLD', 3)
@patch('transform.get_reading_data_from_log_line')
@patch('validate_heart_rate.send_email')
@patch('load.add_reading')
def test_reading_pipeline(mock_load_reading, mock_send_email, mock_transform_reading):
    """Tests function reading_pipeline for patched transform and load functions."""
    log_line = 'this is a log line'
    ride_id = 1
    reading = {'ride_id': ride_id}
    user = {'forename': 'Jane', 'surname': 'Doe', 'min_heart_rate': 60, 'max_heart_rate': 180}
    consecutive_extreme_hrs = [46]

    def reading_values_generator():
        values = [
            {'duration': 1, 'resistance': 30},
            {'heart_rate': 196, 'rpm': 27, 'power': 100},
            {'duration': 2, 'resistance': 42},
            {'heart_rate': 201, 'rpm': 36, 'power': 140},
            {'duration': 3, 'resistance': 10},
            {'heart_rate': 163, 'rpm': 14, 'power': 28}
        ]
        for value in values:
            yield value

    reading_value = reading_values_generator()
    mock_transform_reading.side_effect = (lambda x, y: x | next(reading_value))

    reading = reading_pipeline(log_line, ride_id, reading, user, consecutive_extreme_hrs)
    assert not mock_load_reading.called
    assert not mock_send_email.called
    assert reading == {'ride_id': ride_id, 'duration': 1, 'resistance': 30}
    assert consecutive_extreme_hrs == [46]

    reading = reading_pipeline(log_line, ride_id, reading, user, consecutive_extreme_hrs)
    assert mock_load_reading.call_count == 1
    assert not mock_send_email.called
    assert reading == {'ride_id': ride_id}
    assert consecutive_extreme_hrs == [46, 196]

    reading = reading_pipeline(log_line, ride_id, reading, user, consecutive_extreme_hrs)
    assert mock_load_reading.call_count == 1
    assert not mock_send_email.called
    assert reading == {'ride_id': ride_id, 'duration': 2, 'resistance': 42}
    assert consecutive_extreme_hrs == [46, 196]

    reading = reading_pipeline(log_line, ride_id, reading, user, consecutive_extreme_hrs)
    assert mock_load_reading.call_count == 2
    print(mock_send_email.mock_calls)
    assert mock_send_email.call_count == 1
    assert not consecutive_extreme_hrs
    assert reading == {'ride_id': ride_id}

    consecutive_extreme_hrs = [46, 196]

    reading = reading_pipeline(log_line, ride_id, reading, user, consecutive_extreme_hrs)
    reading = reading_pipeline(log_line, ride_id, reading, user, consecutive_extreme_hrs)
    assert mock_send_email.call_count == 1
    assert not consecutive_extreme_hrs


@patch('pipeline.get_next_log_line')
@patch('pipeline.get_kafka_consumer')
@patch('pipeline.user_pipeline')
@patch('pipeline.ride_pipeline')
@patch('pipeline.reading_pipeline')
@patch('transform.get_bike_serial_number_from_log_line')
@patch('load.add_bike')
def test_pipeline(mock_load_bike, mock_transform_bike, mock_reading_pipeline, mock_ride_pipeline,
                  mock_user_pipeline, mock_get_kafka_consumer, mock_get_next_log_line):
    """Tests the function pipeline directs log lines correctly"""
    mock_get_next_log_line.side_effect = [
        "--------- beginning of main\n",
        "--------- beginning of a new ride\n",
        "2022-07-25 16:13:36.709117 mendoza v9: Getting user data from server..\n",
        "2022-07-25 16:13:37.209120 mendoza v9: \
            [SYSTEM] data = {\"user_id\":815,\"name\":\"Wayne Fitzgerald\",\"gender\":\"male\",\
                \"address\":\"Studio 3,William alley,New Bethan,WR4V 7TA\",\
                \"date_of_birth\":-336700800000,\
                \"email_address\":\"wayne_fitzgerald@hotmail.com\",\"height_cm\":187,\
                \"weight_kg\":52,\"account_create_date\":1641254400000,\"bike_serial\":\"SN0000\",\
                \"original_source\":\"offline\"}\n",
        "2022-07-25 16:13:31.709082 mendoza v9: \
            [INFO]: Ride - duration = 454.0; resistance = 60\n",
        "2022-07-25 16:13:32.209086 mendoza v9: \
            [INFO]: Telemetry - hrt = 0; rpm = 30; power = 11.22896664\n",
        "2022-07-25 16:13:32.709090 mendoza v9: \
            [INFO]: Ride - duration = 455.0; resistance = 60\n",
        "2022-07-25 16:13:33.209093 mendoza v9: \
            [INFO]: Telemetry - hrt = 0; rpm = 30; power = 11.22896664\n",
        "2022-07-25 16:13:33.709097 mendoza v9: \
            [INFO]: Ride - duration = 456.0; resistance = 60\n",
        "2022-07-25 16:13:34.209100 mendoza v9: \
            [INFO]: Telemetry - hrt = 0; rpm = 32; power = 12.54785501\n",
        "2022-07-25 16:13:34.709104 mendoza v9: \
            [INFO]: Ride - duration = 457.0; resistance = 60\n",
        "2022-07-25 16:13:35.209108 mendoza v9: \
            [INFO]: Telemetry - hrt = 0; rpm = 29; power = 10.59008846\n",
        "--------- beginning of main\n",
        "--------- beginning of a new ride\n",
        "2022-07-25 16:13:36.709117 mendoza v9: Getting user data from server..\n",
        "2022-07-25 16:13:37.209120 mendoza v9: \
            [SYSTEM] data = {\"user_id\":815,\"name\":\"Wayne Fitzgerald\",\"gender\":\"male\",\
                \"address\":\"Studio 3,William alley,New Bethan,WR4V 7TA\",\
                \"date_of_birth\":-336700800000,\
                \"email_address\":\"wayne_fitzgerald@hotmail.com\",\"height_cm\":187,\
                \"weight_kg\":52,\"account_create_date\":1641254400000,\"bike_serial\":\"SN0000\",\
                \"original_source\":\"offline\"}\n",
        "2022-07-25 16:13:37.709125 mendoza v9: \
            [INFO]: Ride - duration = 1.0; resistance = 30\n",
        "2022-07-25 16:13:38.209128 mendoza v9: \
            [INFO]: Telemetry - hrt = 0; rpm = 0; power = 0.0\n",
        "2022-07-25 16:13:38.709132 mendoza v9: \
            [INFO]: Ride - duration = 2.0; resistance = 30\n",
        "2022-07-25 16:13:39.209136 mendoza v9: \
            [INFO]: Telemetry - hrt = 84; rpm = 0; power = 0.0\n",
        "2022-07-25 16:13:39.709139 mendoza v9: \
            [INFO]: Ride - duration = 3.0; resistance = 30\n",
        "2022-07-25 16:13:40.209143 mendoza v9: \
            [INFO]: Telemetry - hrt = 84; rpm = 14; power = 1.648772319\n",
        "2022-07-25 16:13:40.709152 mendoza v9: \
            [INFO]: Ride - duration = 4.0; resistance = 30\n",
        "2022-07-25 16:13:41.209157 mendoza v9: \
            [INFO]: Telemetry - hrt = 84; rpm = 27; power = 5.092422057\n",
        "2022-07-25 16:13:41.709160 mendoza v9: \
            [INFO]: Ride - duration = 5.0; resistance = 30\n",
        "2022-07-25 16:13:42.209164 mendoza v9: \
            [INFO]: Telemetry - hrt = 84; rpm = 34; power = 7.473425372\n",
        "2022-07-25 16:13:42.709168 mendoza v9: \
            [INFO]: Ride - duration = 6.0; resistance = 30\n",
        "2022-07-25 16:13:43.209171 mendoza v9: \
            [INFO]: Telemetry - hrt = 85; rpm = 38; power = 8.962429663\n",
        "--------- beginning of main\n",
        "--------- beginning of a new ride\n",
        "2022-07-25 16:13:36.709117 mendoza v9: Getting user data from server..\n",
        "2022-07-25 16:13:37.209120 mendoza v9: \
            [SYSTEM] data = {\"user_id\":815,\"name\":\"Wayne Fitzgerald\",\"gender\":\"male\",\
                \"address\":\"Studio 3,William alley,New Bethan,WR4V 7TA\",\
                \"date_of_birth\":-336700800000,\
                \"email_address\":\"wayne_fitzgerald@hotmail.com\",\"height_cm\":187,\
                \"weight_kg\":52,\"account_create_date\":1641254400000,\"bike_serial\":\"SN0000\",\
                \"original_source\":\"offline\"}\n",
        "2022-07-25 16:13:39.709139 mendoza v9: \
            [INFO]: Ride - duration = 3.0; resistance = 30\n",
        "2022-07-25 16:13:40.209143 mendoza v9: \
            [INFO]: Telemetry - hrt = 84; rpm = 14; power = 1.648772319\n",
        "2022-07-25 16:13:40.709152 mendoza v9: \
            [INFO]: Ride - duration = 4.0; resistance = 30\n",
        "2022-07-25 16:13:41.209157 mendoza v9: \
            [INFO]: Telemetry - hrt = 84; rpm = 27; power = 5.092422057\n",
        "2022-07-25 16:13:41.709160 mendoza v9: \
            [INFO]: Ride - duration = 5.0; resistance = 30\n",
        "2022-07-25 16:13:42.209164 mendoza v9: \
            [INFO]: Telemetry - hrt = 84; rpm = 34; power = 7.473425372\n",
        "2022-07-25 16:13:42.709168 mendoza v9: \
            [INFO]: Ride - duration = 6.0; resistance = 30\n",
        "2022-07-25 16:13:43.209171 mendoza v9: \
            [INFO]: Telemetry - hrt = 85; rpm = 38; power = 8.962429663\n",
        ValueError
    ]
    try:
        pipeline()
    except ValueError:
        pass

    assert mock_get_next_log_line.call_count == 40 + 1
    assert mock_user_pipeline.call_count == 3
    assert mock_transform_bike.call_count == 3
    assert mock_load_bike.call_count == 3
    assert mock_ride_pipeline.call_count == 3
    assert mock_reading_pipeline.call_count == 40 - 4*3
