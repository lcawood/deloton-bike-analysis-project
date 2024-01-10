"""
Pipeline script to fetch data from the kafka, extract and process bike, address, rider, ride, and
reading data from it, and upload this to the database. This is done in two main swathes; first, any
backlog of data in the Kafka stream is processed as efficiently as possible, and then a different
script (with real time (ish) heart rate alerts) is used to process the live data.
"""

from datetime import datetime
from functools import partial
import json
import logging
from multiprocessing import Pool, Process
import os

from botocore.exceptions import ClientError
from confluent_kafka import Consumer, KafkaException, Message
from dotenv import load_dotenv
import pandas as pd
from psycopg2.extensions import connection

import load
import transform
import validate_heart_rate
from database_functions import get_database_connection


GROUP_ID = "pipeline_zeta"
READINGS_CSV = "readings.csv"
EXTREME_HR_COUNT_THRESHOLD = 3



class KafkaConnection():
    """Class to group functions used to interact with the Kafka consumer object."""

    def __init__(self, group_id: int = GROUP_ID):
        """
        Saves consumer object to class variables, as well as giving message an initial value of
        None.
        """
        self._consumer = self._get_kafka_consumer(group_id)
        self._last_message = None
        self._pre_system_messages = [None, None]


    @staticmethod
    def _get_kafka_consumer(group_id: str) -> Consumer:
        """
        Protected function to return a consumer for the Kafka cluster specified in .env; logs error
        and raises error if a KafkaException occurs.
        """
        try:
            kafka_config = {
                'bootstrap.servers': os.environ['BOOTSTRAP_SERVERS'],
                'security.protocol': os.environ['SECURITY_PROTOCOL'],
                'sasl.mechanisms': os.environ['SASL_MECHANISM'],
                'sasl.username': os.environ['USERNAME'],
                'sasl.password': os.environ['PASSWORD'],
                'group.id': group_id,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': 'false'
            }
            consumer = Consumer(kafka_config)
            consumer.subscribe([os.environ['KAFKA_TOPIC']])
            return consumer

        except KafkaException as error:
            logging.critical("Unable to connect to Kafka stream.")
            raise error


    @staticmethod
    def _get_log_line_from_message(message: Message | None) -> str:
        """
        Protected function to return log line from kafka stream message; returns None if no 'log'
        key in message, and returns the message unaltered if it is not a Kafka Message.
        """
        if isinstance(message, Message):
            return json.loads(message.value().decode()).get('log')
        return message


    def get_next_log_line(self, commit_on_system_line: bool = True) -> str:
        """
        Public function to retrieve and return the next non-null log line from kafka stream;
        
        Stores last message retrieved as class variable, which (if commit_on_system_line == True)
        is used to manually set the (group_id specific) offset in the Kafka partition being
        accessed when a [SYSTEM] log line is retrieved.
        """

        log_line = None
        while not log_line:
            message = self._consumer.poll(1)
            log_line = self._get_log_line_from_message(message)

        if ('[SYSTEM]' in log_line) and self._last_message:
            self._pre_system_messages.pop(0)
            self._pre_system_messages.append(self._last_message)
            if commit_on_system_line:
                self.save_stream_position()

        self._last_message = message

        return log_line
    

    def save_stream_position(self, system_message_before_last: bool = False):
        """
        Function to use self._pre_system_messages to save the stream to return the last message (or
        the message before last, if system_message_before_last is True) fetched the next time the
        partition is accessed.
        """
        print('saving')
        if system_message_before_last:
            message = self._pre_system_messages[-2]
        else:
            message = self._pre_system_messages[-1]
        if message:
            self._consumer.commit(message, asynchronous=True)



class Pipeline():
    """Class to group functions used in the running of the (live) pipeline."""

    def __init__(self, kafka_connection: KafkaConnection, db_connection: connection,
                 extreme_hr_count_threshold: int = EXTREME_HR_COUNT_THRESHOLD):
        self._kafka_connection = kafka_connection
        self._extreme_hr_count_threshold = extreme_hr_count_threshold

        self._db_connection = db_connection

        self._log_line = ""
        self._address_id = None
        self._rider = None
        self._bike_id = None
        self._ride = None
        self._consecutive_extreme_hrs = []


    def _address_pipeline(self):
        """
        Protected function to extract address information from log line, add (or locate) it in the
        db, and save the resultant address_id in the instance variables.
        """
        address = transform.get_address_from_log_line(self._log_line)
        self._address_id = load.add_address(self._db_connection, address)


    def _rider_pipeline(self):
        """
        Protected function to extract rider information from log_line, add it to the db, and save
        the resultant rider dictionary (with max and min heart rate fields, and address_id) to the
        instance variables.
        """
        self._rider = transform.get_rider_from_log_line(self._log_line)
        self._rider['address_id'] = self._address_id

        load.add_rider(self._db_connection, self._rider)

        self._rider['min_heart_rate'] = validate_heart_rate.calculate_min_heart_rate(self._rider)
        self._rider['max_heart_rate'] = validate_heart_rate.calculate_max_heart_rate(self._rider)


    def _bike_pipeline(self):
        """
        Protected function to extract bike serial number from log line, upload it to the db (or
        locate it if it already exists), and add the returned bike_id number to the class
        variables.
        """
        bike_serial_number = transform.get_bike_serial_number_from_log_line(self._log_line)
        self._bike_id = load.add_bike(self._db_connection, bike_serial_number)


    def _ride_pipeline(self):
        """
        Function to extract ride info from the given log line, upload it to the db, and return the
        id of the ride.
        """
        self._ride = transform.get_ride_data_from_log_line(self._log_line)
        self._ride['bike_id'] = self._bike_id
        self._ride['ride_id'] = load.add_ride(self._db_connection, self._ride)


    def _reading_pipeline(self):
        """
        Protected function to extract reading data from class variable log_line (which should be a
        pair of reading lines), add it to reading dict, upload this to the db, and alert the rider
        by email if their heart rate has had an extreme value for EXTREME_HR_COUNT_THRESHOLD
        readings.
        """
        reading = transform.get_data_from_reading_line_pair(self._log_line,
                                                            self._ride['start_time'])
        reading['ride_id'] = self._ride['ride_id']

        load.add_reading(self._db_connection, reading)

        if (reading['heart_rate'] == 0) or \
            (self._rider['min_heart_rate'] <= reading['heart_rate'] \
             <= self._rider['max_heart_rate']):
            self._consecutive_extreme_hrs.clear()

        else:
            self._consecutive_extreme_hrs.append(reading['heart_rate'])

        if len(self._consecutive_extreme_hrs) == self._extreme_hr_count_threshold:
            try:
                validate_heart_rate.send_email(self._rider, self._consecutive_extreme_hrs)
            except ClientError as e:
                logging.error('Unable to send email; %s', str(e))

            self._consecutive_extreme_hrs.clear()


    def pipeline(self):
        """
        Public function to run the main pipeline; retrieves messages from Kafka stream, utilises
        the transform module to extract and process the data, and uses the load module to upload to
        the db.
        """

        while True:

            self._log_line = self._kafka_connection.get_next_log_line()

            if '[SYSTEM]' in self._log_line:
                self._consecutive_extreme_hrs = []
                self._address_pipeline()
                self._rider_pipeline()
                self._bike_pipeline()
                self._ride_pipeline()

                logging.info("Processing ride with id %s, starting on %s...",
                             str(self._ride['ride_id']),
                             str(self._ride['start_time']))

            if ('[INFO]: Ride' in self._log_line) and self._rider:
                log_line = self._kafka_connection.get_next_log_line()
                if '[INFO]: Telemetry' in log_line:
                    self._log_line += log_line
                    self._reading_pipeline()



class BackfillPipeline(Pipeline):
    """
    A subclass of Pipeline used to catch on old data; optimised for speed, and with no HR
    functionality.
    """

    def __init__(self, kafka_connection: KafkaConnection, db_connection: connection,
                 stop_date: datetime = None, readings_csv_file: str = READINGS_CSV):
        """
        stop_date is the datetime the readings' log times need to exceed to halt the pipeline()
        function (defined below).
        """
        super().__init__(kafka_connection, db_connection)
        del self._consecutive_extreme_hrs   # Inherited from Pipeline, but unused here.

        self._stop_date = stop_date
        self._readings_csv_file = readings_csv_file


    def _rider_pipeline(self) -> dict:
        """
        Protected function to extract rider and address information from log_line, add it to the
        db, and add rider dictionary (without max and min heart rate fields) to class variables.
        """
        self._rider = transform.get_rider_from_log_line(self._log_line)
        self._rider['address_id'] = self._address_id
        load.add_rider(self._db_connection, self._rider)


    @staticmethod
    def _readings_pipeline(reading_line_pair: str, ride_id: int, start_time: datetime) -> dict:
        """
        Protected function to extract reading data from a pair of reading_lines (concatenated),
        add the given ride_id, and return as dict.

        Static method as it's used in multiprocessing (this stops incidental additional instances
        of the class being spun up).
        """
        reading = transform.get_data_from_reading_line_pair(reading_line_pair, start_time)
        reading['ride_id'] = ride_id
        return reading


    @classmethod
    def _process_readings(cls, reading_line_pairs: list[str], ride_id: int, start_time: datetime,
                          readings_csv_file: str):
        """
        Protected function to process a list of reading_line_pairs (where each string is two halves
        of a reading concatenated), save the resultant dataframe to a csv, and then copy said csv
        to the database.

        Class method as it's used in multiprocessing (to stop incidental additional instances
        of the class being spun up), but still needs access to _reading_pipeline().
        """
        partial_reading_pipeline = partial(
            cls._readings_pipeline, ride_id = ride_id, start_time = start_time)

        with Pool() as pool:
            readings = pd.DataFrame(pool.map(partial_reading_pipeline, reading_line_pairs))

        readings.to_csv(readings_csv_file, index=False)


    def _check_pipeline_stop_conditions(self) -> bool:
        """
        Helper function for pipeline() to determine if the pipeline should (start to or continue
        to) stop.
        """
        if self._ride is None:
            return False
        
        if self._stop_date and (self._stop_date < self._ride['start_time']):
            return True
        
        if datetime.now().strftime('%Y-%m-%d %H') == \
            self._ride['start_time'].strftime('%Y-%m-%d %H'):
            return True
        
        return False


    def pipeline(self):
        """
        Public function to run the backfill pipeline; retrieves messages from Kafka stream,
        utilises transform module to extract and process the data from the log lines, and uses load
        module to upload to the db; it does this in batches using multiprocessing.
        """

        r_process = None
        stop_pipeline = False

        if os.path.exists(self._readings_csv_file):
            load.add_readings_from_csv(self._db_connection, self._readings_csv_file)

        while not stop_pipeline:

            stop_pipeline = self._check_pipeline_stop_conditions()

            if stop_pipeline:
                # Finishes processing, and uploads final batch.
                if r_process:
                    r_process.join()
                    r_process.close()
                    load.add_readings_from_csv(self._db_connection, self._readings_csv_file)
            
            elif '[SYSTEM]' in self._log_line:
                # Handles next ride
                self._address_pipeline()
                self._rider_pipeline()
                self._bike_pipeline()
                self._ride_pipeline()

                logging.info("Processing ride with id %s, starting on %s...",
                             str(self._ride['ride_id']), 
                             str(self._ride['start_time']))

                # Concatenates each pair of [INFO] log lines and adds to list
                # until [SYSTEM] log line is hit.
                reading_line_pairs = []
                self._log_line = self._kafka_connection.get_next_log_line(False)
                while '[INFO]' in self._log_line:
                    self._log_line += self._kafka_connection.get_next_log_line(False)
                    reading_line_pairs.append(self._log_line)
                    self._log_line = self._kafka_connection.get_next_log_line(False)

                # Each batch of reading uploads to db use the same .csv file, so each must be
                # allowed to finish before the next begins.
                if r_process:
                    r_process.join()
                    r_process.close()
                    load.add_readings_from_csv(self._db_connection, self._readings_csv_file)

                    self._kafka_connection.save_stream_position(True)

                # Multiprocessing used to cycle around and retrieve the next ride's Kafka messages
                # while the last batch of readings are being processed and uploaded to the db.
                r_process = Process(target=self._process_readings,
                            args=(reading_line_pairs, self._ride['ride_id'],
                                self._ride['start_time'], self._readings_csv_file))
                r_process.start()

            else:
                # Skips over lines which contain neither [SYSTEM] nor [INFO] data.
                self._log_line = self._kafka_connection.get_next_log_line(False)
            
        # Removes utility csv file.
        if os.path.exists(self._readings_csv_file):
            os.remove(self._readings_csv_file)



def main():
    """
    Runs first the backfill pipeline, and then the live pipeline until fatal error or user
    interrupt.
    """
    db_conn = local_db()

    logging.info("Running backfill pipeline...")
    # Distinct Kafka connections must be used to ensure offsetting works.
    BackfillPipeline(KafkaConnection(), db_conn).pipeline()
    logging.info("Backfill pipeline finished.")

    logging.info("Running live pipeline...")
    Pipeline(KafkaConnection(), db_conn).pipeline()


if __name__ == "__main__":

    load_dotenv()
    logging.basicConfig(
        level=logging.INFO
    )
    main()

