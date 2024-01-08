"""
Pipeline script to fetch data from the kafka, extract and process bike, address, rider, ride, and
reading data from it, and upload this to the database. This is done in two main swathes; first, any
backlog of data in the Kafka stream is processed as efficiently as possible, and then a different
script (with real time (ish) heart rate alerts) is used to process the live data.

The entire functionality of the module is contained within the main() function (which is called by
default), which roughly follows the following steps:

 - Establish a connection to the Kafka stream;

 - first run pipeline() from the class BackfillPipeline to catch up on historic data from the kafka
 stream;
    - this class inherits from Pipeline, and is a special implementation of it's
    functionality without the heart rate alert system, and optimised for speed.
    - Operates in batches by ride.

 - run pipeline() from the class Pipeline to retrieve log lines, and then transform
 and upload the relevant data to the database (using functions from the `transform.py` and
 `load.py` files as necessary). Also within this pipeline:
    - User's max and min heart rates are calculated using functions from `validate_heart_rate.py`,
    and their heart rate in the readings compared against them; if it lies outside the healthy
    range too many times in a row (EXTREME_HR_COUNT_THRESHOLD), the validate_heart_rate function
    send_email is used to alert the rider;
    - This function handles one log line at a time.

"""

from datetime import datetime
from functools import partial
import json
import logging
from multiprocessing import Pool, Process
import os
import pandas as pd

from botocore.exceptions import ClientError
from confluent_kafka import Consumer, KafkaException, Message
from dotenv import load_dotenv

import load
import transform
import validate_heart_rate


GROUP_ID = "pipeline_zeta"
READINGS_CSV = "readings.csv"
EXTREME_HR_COUNT_THRESHOLD = 3



class KafkaConnection():
    """Class to groups functions used to interact with the Kafka consumer object."""

    def __init__(self, group_id: int = GROUP_ID):
        """
        Saves consumer object to class variables, as well as giving message an initial value of
        None.
        """
        self.consumer = self._get_kafka_consumer(group_id)
        self.message = None


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


    def get_next_log_line(self) -> str:
        """
        Public function to retrieve and return the next non-null log line from kafka stream;
        
        Stores last message retrieved as class variable, which is used to manually set the
        (group_id specific) offset in the Kafka partition being accessed when a [SYSTEM] log line
        is retrieved.
        """

        log_line = None
        while not log_line:
            message = self.consumer.poll(1)
            log_line = self._get_log_line_from_message(message)

        if self.message and ('[SYSTEM]' in log_line):
            self.consumer.commit(self.message, asynchronous=False)

        self.message = message

        return log_line



class Pipeline():
    """Class to group functions used in the running of the (live) pipeline."""

    def __init__(self, kafka_connection: KafkaConnection):
        self.kafka_connection = kafka_connection
        self.log_line = ""
        self.address_id = None
        self.rider = None
        self.bike_id = None
        self.ride = None
        self.consecutive_extreme_hrs = []


    def _address_pipeline(self):
        """
        Protected function to extract address information from log line, add (or locate) it in the
        db, and save the resultant address_id in the class variables.
        """
        address = transform.get_address_from_log_line(self.log_line)
        self.address_id = load.add_address(address)


    def _rider_pipeline(self) -> dict:
        """
        Protected function to extract rider information from log_line, add it to the db, and save
        the resultant rider dictionary (with max and min heart rate fields, and address_id) to the
        class variables.
        """
        self.rider = transform.get_rider_from_log_line(self.log_line)
        self.rider['address_id'] = self.address_id

        load.add_rider(self.rider)

        self.rider['min_heart_rate'] = validate_heart_rate.calculate_min_heart_rate(self.rider)
        self.rider['max_heart_rate'] = validate_heart_rate.calculate_max_heart_rate(self.rider)


    def _bike_pipeline(self):
        """
        Protected function to extract bike serial number from log line, upload it to the db (or
        locate it if it already exists), and add the returned bike_id number to the class
        variables.
        """
        bike_serial_number = transform.get_bike_serial_number_from_log_line(self.log_line)
        self.bike_id = load.add_bike(bike_serial_number)


    def _ride_pipeline(self) -> dict:
        """
        Function to extract ride info from the given log line, upload it to the db, and return the
        id of the ride.
        """
        self.ride = transform.get_ride_data_from_log_line(self.log_line)
        self.ride['bike_id'] = self.bike_id
        self.ride['ride_id'] = load.add_ride(self.ride)


    def __reading_pipeline(self) -> dict:
        """
        Private function to extract reading data from class variable log_line (which should be a
        pair of reading lines), add it to reading dict, upload this to the db, and alert the rider
        by email if their heart rate has had an extreme value for EXTREME_HR_COUNT_THRESHOLD
        readings.

        Private as it is neither accessed nor overwritten in the subclass BackfillPipeline (as this
        deals with readings, never the singular).
        """
        reading = transform.get_data_from_reading_line_pair(self.log_line, self.ride['start_time'])

        load.add_reading(reading)

        if (reading['heart_rate'] == 0) or \
            (self.rider['min_heart_rate'] <= reading['heart_rate'] <= self.rider['max_heart_rate']):
            self.consecutive_extreme_hrs.clear()
        else:
            self.consecutive_extreme_hrs.append(reading['heart_rate'])

        if len(self.consecutive_extreme_hrs) == EXTREME_HR_COUNT_THRESHOLD:
            try:
                validate_heart_rate.send_email(self.rider, self.consecutive_extreme_hrs)
            except ClientError:
                logging.error('Unable to send email.')

            self.consecutive_extreme_hrs.clear()


    def pipeline(self):
        """
        Public function to run the main pipeline; retrieves messages from Kafka stream, utilises
        the transform module to extract and process the data, and uses the load module to upload to
        the db.
        """

        while True:

            self.log_line = self.kafka_connection.get_next_log_line()

            if '[SYSTEM]' in self.log_line:
                self.consecutive_extreme_hrs = []
                self._address_pipeline()
                self._rider_pipeline()
                self._bike_pipeline()
                self._ride_pipeline()

            if ('[INFO]: Ride' in self.log_line) and self.rider:
                log_line = self.kafka_connection.get_next_log_line()
                if '[INFO]: Telemetry' in log_line:
                    self.log_line += log_line
                    self.__reading_pipeline()



class BackfillPipeline(Pipeline):
    """
    A subclass of Pipeline used to catch on old data; optimised for speed, and with no HR
    functionality.
    """

    def __init__(self, kafka_connection: KafkaConnection, stop_date: datetime = None,
                 readings_csv_file: str = READINGS_CSV):
        """
        stop_date is the datetime the readings' log times need to exceed to halt the pipeline()
        function (defined below).
        """
        super().__init__(kafka_connection)
        self.stop_date = stop_date
        self.readings_csv_file = readings_csv_file


    def _rider_pipeline(self) -> dict:
        """
        Protected function to extract rider and address information from log_line, add it to the
        db, and add rider dictionary (without max and min heart rate fields) to class variables.
        """
        self.rider = transform.get_rider_from_log_line(self.log_line)
        address = transform.get_address_from_log_line(self.log_line)
        self.rider['address_id'] = load.add_address(address)
        load.add_rider(self.rider)


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
        of the class being spun up), but still needs access to _reading_pipeline.
        """
        partial_reading_pipeline = partial(
            cls._readings_pipeline, ride_id = ride_id, start_time = start_time)

        with Pool() as pool:
            readings = pd.DataFrame(pool.map(partial_reading_pipeline, reading_line_pairs))

        readings.to_csv(readings_csv_file, index=False)
        load.add_readings_from_csv(readings_csv_file)


    def pipeline(self):
        """
        Public function to run the backfill pipeline; retrieves messages from Kafka stream,
        utilises transform module to extract and process the data from the log lines, and uses load
        module to upload to the db; it does this in batches using multiprocessing.
        """

        self.log_line = ""
        p = None

        if os.path.exists(self.readings_csv_file):
            load.add_readings_from_csv(self.readings_csv_file)

        while True:

            if '[SYSTEM]' in self.log_line:
                self._address_pipeline()
                self._rider_pipeline()
                self._bike_pipeline()
                self._ride_pipeline()

                # Stops historical_pipeline automatically if caught up to specified datetime.
                if self.stop_date:
                    if self.stop_date < self.ride['start_time']:
                        return None
                elif datetime.now().strftime('%Y-%m-%d %H') in log_line:
                    return None

                # Concatenates each pair of [INFO] log lines and adds to list
                # until [SYSTEM] log line is hit.
                reading_line_pairs = []
                log_line = self.kafka_connection.get_next_log_line()
                while '[INFO]' in log_line:
                    log_line += self.kafka_connection.get_next_log_line()
                    reading_line_pairs.append(log_line)
                    log_line = self.kafka_connection.get_next_log_line()

                # Each batch of reading uploads to db use the same .csv file, so each must be
                # allowed to finish before the next begins.
                if p:
                    p.join()
                    p.close()

                    print("Start time of ride last processed: ", self.ride['start_time'])

                # Multiprocessing used to cycle around and retrieve the next ride's Kafka messages
                # while the last batch of readings are being processed and uploaded to the db.
                p = Process(target=self._process_readings,
                            args=(reading_line_pairs, self.ride['ride_id'], self.ride['start_time'],
                                  self.readings_csv_file))
                p.start()

            else:
                # Skips over lines which contain neither [SYSTEM] nor [INFO] data.
                log_line = self.kafka_connection.get_next_log_line()



def main():
    """
    Runs first the backfill pipeline, and then the live pipeline until fatal error or user
    interrupt.
    """
    kafka_conn = KafkaConnection()

    logging.info("Running backfill_pipeline...")
    BackfillPipeline(kafka_conn).pipeline()
    logging.info("backfill_pipeline finished.")

    logging.info("Running live_pipeline...")
    Pipeline(kafka_conn).pipeline()


if __name__ == "__main__":
    load_dotenv()
    main()
