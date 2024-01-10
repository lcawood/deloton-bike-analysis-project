"""Contains unit tests for pipeline.py"""

# Needs this to perform unit tests on protected members:
# pylint: disable=W0212

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from pipeline import KafkaConnection, Pipeline, BackfillPipeline


class TestKafkaConnection():
    """Unit tests for the class KafkaConnection."""

    @patch('pipeline.KafkaConnection._get_kafka_consumer')
    @patch('pipeline.KafkaConnection._get_log_line_from_message')
    def test_get_next_log_line(self, mock_get_log_line_from_message, mock_get_kafka_consumer):
        """Tests function _get_next_log_line."""
        test_consumer = MagicMock()
        test_consumer.poll.side_effect = [
            {},
            None,
            {'not a log line': 42},
            {'log': 'this is a log line'},
            {'elves': 'magical creatures'},
            {'log': 'this is a [SYSTEM] log line'},
            {'log': 'this, too, is a log line'}
        ]
        mock_get_kafka_consumer.return_value = test_consumer
        mock_get_log_line_from_message.side_effect = \
            lambda message_dict: message_dict.get('log') if message_dict else None

        test_kafka_conn = KafkaConnection("testing")

        assert test_kafka_conn.get_next_log_line() == 'this is a log line'
        assert test_kafka_conn._pre_system_messages == [None, None]
        assert test_kafka_conn._last_message == {'log': 'this is a log line'}
        test_consumer.commit.assert_not_called()

        assert test_kafka_conn.get_next_log_line() == 'this is a [SYSTEM] log line'
        assert test_kafka_conn._pre_system_messages == [None, {'log': 'this is a log line'}]
        test_consumer.commit.assert_called_once_with({'log': 'this is a log line'},
                                                     asynchronous=False)

        assert test_kafka_conn.get_next_log_line() == 'this, too, is a log line'
        assert test_kafka_conn._pre_system_messages == [None, {'log': 'this is a log line'}]
        assert test_consumer.commit.call_count == 1



class TestPipeline():
    """Units tests for the class Pipeline."""

    @pytest.fixture(scope='function')
    def test_pipeline(self):
        """
        Returns a test Pipeline for use of tests in this class, with mocked Kafka and db
        connections.
        """
        test_kafka_conn = MagicMock()
        test_db_conn = MagicMock()
        return Pipeline(test_kafka_conn, test_db_conn, 3)


    @patch('transform.get_address_from_log_line')
    @patch('load.add_address')
    def test_address_pipeline(self, mock_load_address, mock_transform_address, test_pipeline):
        """Tests function _address_pipeline."""
        fake_address = {
            'first_line': '4 Privet Drive',
            'second_line': None,
            'city': 'Little Whinging',
            'postcode': 'TW16 9ZZ'
        }
        mock_transform_address.return_value = fake_address
        mock_load_address.return_value = 8

        test_pipeline._log_line = "this is a log line"

        test_pipeline._address_pipeline()

        mock_transform_address.assert_called_once_with('this is a log line')
        mock_load_address.assert_called_once_with(test_pipeline._db_connection, fake_address)
        assert test_pipeline._address_id == 8


    @patch('transform.get_rider_from_log_line')
    @patch('load.add_rider')
    @patch('validate_heart_rate.calculate_max_heart_rate')
    @patch('validate_heart_rate.calculate_min_heart_rate')
    def test_rider_pipeline(self, mock_min_heart_rate, mock_max_heart_rate, mock_load_rider,
                            mock_transform_rider, test_pipeline):
        """
        Tests _rider_pipeline function for patched transform, validate_heart_rate and load
        functions.
        """

        test_pipeline._log_line = 'I am a log line'
        test_pipeline._address_id = 1

        mock_transform_rider.return_value = {'rider_id': 7, 'forename': 'John', 'surname': 'Doe'}
        mock_min_heart_rate.return_value = 60
        mock_max_heart_rate.return_value = 180

        test_pipeline._rider_pipeline()

        mock_load_rider.assert_called_once()

        assert test_pipeline._rider == {
            'rider_id': 7,
            'forename': 'John',
            'surname': 'Doe',
            'address_id': 1,
            'min_heart_rate': 60,
            'max_heart_rate': 180
            }


    @patch('transform.get_bike_serial_number_from_log_line')
    @patch('load.add_bike')
    def test_bike_pipeline(self, mock_load_bike, mock_transform_bike, test_pipeline):
        """Tests function _bike_pipeline."""
        fake_bike_serial_num = "SN24601"
        mock_transform_bike.return_value = fake_bike_serial_num
        mock_load_bike.return_value = 1862

        test_pipeline._log_line = "this is a log line"

        test_pipeline._bike_pipeline()

        mock_transform_bike.assert_called_once_with('this is a log line')
        mock_load_bike.assert_called_once_with(test_pipeline._db_connection, fake_bike_serial_num)
        assert test_pipeline._bike_id == 1862


    @patch('transform.get_ride_data_from_log_line')
    @patch('load.add_ride')
    def test_ride_pipeline(self, mock_load_ride, mock_transform_ride, test_pipeline):
        """Tests function _ride_pipeline for patched transform and load functions."""
        test_pipeline._log_line = "this is a log line"
        test_pipeline._bike_id = 2

        mock_load_ride.return_value = 7
        mock_transform_ride.return_value = {'key': 'value in a dictionary of ride info'}

        test_pipeline._ride_pipeline()

        mock_transform_ride.assert_called_once_with(test_pipeline._log_line)
        mock_load_ride.assert_called_once_with(test_pipeline._db_connection, {
            'key': 'value in a dictionary of ride info',
            'bike_id': 2, 
            'ride_id': 7
            })


    @patch('transform.get_data_from_reading_line_pair')
    @patch('validate_heart_rate.send_email')
    @patch('load.add_reading')
    def test_reading_pipeline(self, mock_load_reading, mock_send_email, mock_transform_reading,
                              test_pipeline):
        """Tests function _reading_pipeline for patched transform and load functions."""
        start_time = datetime.now()

        test_pipeline._log_line = 'this is a log line'
        test_pipeline._rider = {'forename': 'Jane',
                                'surname': 'Doe',
                                'min_heart_rate': 60,
                                'max_heart_rate': 180}
        test_pipeline._ride = {'ride_id': 1, 'start_time': start_time}
        test_pipeline._consecutive_extreme_hrs = [46]

        mock_transform_reading.side_effect = [
                {'duration': 1, 'resistance': 30, 'elapsed_time': 6, 'heart_rate': 196, 'rpm': 27,
                 'power': 100},
                {'duration': 2, 'resistance': 42, 'elapsed_time': 6, 'heart_rate': 201, 'rpm': 36,
                 'power': 140},
                {'duration': 3, 'resistance': 10, 'elapsed_time': 6, 'heart_rate': 163, 'rpm': 14,
                 'power': 28}
            ]

        test_pipeline._reading_pipeline()
        mock_transform_reading.assert_called_once_with('this is a log line', start_time)
        mock_load_reading.assert_called_once_with(test_pipeline._db_connection,
                                                  {'duration': 1, 'resistance': 30,
                                                   'elapsed_time': 6, 'heart_rate': 196,
                                                   'rpm': 27, 'power': 100, 'ride_id': 1})
        assert not mock_send_email.called
        assert test_pipeline._consecutive_extreme_hrs == [46, 196]

        test_pipeline._log_line = 'this is another log line!'
        test_pipeline._reading_pipeline()
        assert mock_transform_reading.call_count == 2
        mock_transform_reading.assert_called_with('this is another log line!', start_time)
        assert mock_load_reading.call_count == 2
        mock_load_reading.assert_called_with(test_pipeline._db_connection,
                                             {'duration': 2, 'resistance': 42,
                                              'elapsed_time': 6, 'heart_rate': 201,
                                              'rpm': 36, 'power': 140, 'ride_id': 1})
        mock_send_email.assert_called_once_with(test_pipeline._rider,
                                                test_pipeline._consecutive_extreme_hrs)
        assert test_pipeline._consecutive_extreme_hrs == []

        test_pipeline._consecutive_extreme_hrs = [46, 196]

        test_pipeline._reading_pipeline()
        assert mock_send_email.call_count == 1
        assert test_pipeline._consecutive_extreme_hrs == []


    @patch('pipeline.Pipeline._address_pipeline')
    @patch('pipeline.Pipeline._rider_pipeline')
    @patch('pipeline.Pipeline._bike_pipeline')
    @patch('pipeline.Pipeline._ride_pipeline')
    @patch('pipeline.Pipeline._reading_pipeline')
    def test_live_pipeline(self, mock_reading_pipeline, mock_bike_pipeline, mock_ride_pipeline,
                           mock_rider_pipeline, mock_address_pipeline, test_pipeline):
        """
        Tests the function pipeline() directs log lines correctly starting at the beginning of a
        ride.
        """
        test_pipeline._kafka_connection.get_next_log_line.side_effect = [
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
        test_pipeline._rider = MagicMock()
        test_pipeline._ride = MagicMock()

        try:
            test_pipeline.pipeline()
        except ValueError:
            pass

        assert test_pipeline._kafka_connection.get_next_log_line.call_count == 40 + 1
        assert mock_address_pipeline.call_count == 3
        assert mock_rider_pipeline.call_count == 3
        assert mock_bike_pipeline.call_count == 3
        assert mock_ride_pipeline.call_count == 3
        assert mock_reading_pipeline.call_count == 14



class TestBackfillPipeline():
    """Unit tests for BackfillPipeline (where it's functionality differs from Pipeline."""

    @pytest.fixture(scope='function')
    def test_bf_pipeline(self):
        """
        Returns a test Pipeline for use of tests in this class, with mocked Kafka and db
        connections.
        """
        test_kafka_conn = MagicMock()
        test_db_conn = MagicMock()
        return BackfillPipeline(test_kafka_conn, test_db_conn)


    @patch('transform.get_rider_from_log_line')
    @patch('load.add_rider')
    def test_rider_pipeline(self, mock_load_rider,
                            mock_transform_rider, test_bf_pipeline):
        """
        Tests _rider_pipeline function for patched transform and load functions.
        """

        test_bf_pipeline._log_line = 'I am a log line'
        test_bf_pipeline._address_id = 1

        mock_transform_rider.return_value = {'rider_id': 7, 'forename': 'John', 'surname': 'Doe'}

        test_bf_pipeline._rider_pipeline()

        rider = {
            'rider_id': 7,
            'forename': 'John',
            'surname': 'Doe',
            'address_id': 1
            }

        mock_load_rider.assert_called_once_with(test_bf_pipeline._db_connection, rider)

        assert test_bf_pipeline._rider == rider


    @patch('transform.get_data_from_reading_line_pair')
    def test_readings_pipeline(self, mock_transform_reading, test_bf_pipeline):
        """Tests the (static) function _readings_pipeline."""

        mock_transform_reading.return_value = {'duration': 2, 'resistance': 42,
                                              'elapsed_time': 6, 'heart_rate': 201,
                                              'rpm': 36, 'power': 140}
        start_time = datetime.now()
        reading = test_bf_pipeline._readings_pipeline("this is a pair \n of log lines.",
                                                   42,
                                                   start_time)
        mock_transform_reading.assert_called_once_with("this is a pair \n of log lines.",
                                                       start_time)
        assert reading == {'duration': 2, 'resistance': 42,
                           'elapsed_time': 6, 'heart_rate': 201,
                           'rpm': 36, 'power': 140, 'ride_id': 42}
