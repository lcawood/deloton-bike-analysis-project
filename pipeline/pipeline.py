"""
Pipeline script to run first backfill_pipeline() from backfill_pipeline.py (to catch up on historic
data from the kafka stream; described in docstring for said file), and then live_pipeline()
(described below, and defined further below).

live_pipeline():
 - Pipeline script to establish connection to Kafka stream, retrieve log lines, and then transform
 and upload the relevant data to the database (using functions from the `transform.py` and
 `load.py` files as necessary).
 - User's max and min heart rates are calculated using functions from validate_heart_rate, and
 their heart rate given in the readings compared against them; if it above or below the healthy
 range too many times in a row (EXTREME_HR_COUNT_THRESHOLD), the validate_heart_rate function
 send_email is used to alert the rider.
"""

from datetime import datetime
import json
import logging
from os import environ

from botocore.exceptions import ClientError

from confluent_kafka import Consumer, KafkaException, Message
from dotenv import load_dotenv
import load
import transform
import validate_heart_rate

import backfill_pipeline

GROUP_ID = "pipeline_zeta"
EXTREME_HR_COUNT_THRESHOLD = 3


def get_kafka_consumer(group_id: str) -> Consumer:
    """Function to return a consumer for the kafka cluster specified in .env."""
    try:
        kafka_config = {
            'bootstrap.servers': environ['BOOTSTRAP_SERVERS'],
            'security.protocol': environ['SECURITY_PROTOCOL'],
            'sasl.mechanisms': environ['SASL_MECHANISM'],
            'sasl.username': environ['USERNAME'],
            'sasl.password': environ['PASSWORD'],
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': 'false'
        }
        consumer = Consumer(kafka_config)
        consumer.subscribe([environ['KAFKA_TOPIC']])
        return consumer
    except KafkaException as error:
        logging.critical("Unable to connect to Kafka stream.")
        raise error


def get_next_log_line(consumer: Consumer, messages: list[Message]) -> str:
    """Function to retrieve and return next log line from kafka stream."""
    messages.append(None)

    log_line = None
    while not log_line:
        messages[-1] = consumer.poll(1)
        log_line = get_log_line_from_message(messages[-1])

    if len(messages) == 3:
        messages.pop(0)
        if '[SYSTEM]' in log_line:
            consumer.commit(messages[0], asynchronous=False)

    return log_line


def get_log_line_from_message(message: Message | None) -> str:
    """Returns log line from kafka stream message; returns None if no 'log' key in message."""
    if isinstance(message, Message):
        return json.loads(message.value().decode()).get('log')
    return None


def rider_pipeline(log_line: str) -> dict:
    """
    Function to extract rider and address information from log_line, add it to the db, and return
    rider dictionary (with max and min heart rate fields).
    """
    rider = transform.get_rider_from_log_line(log_line)
    address = transform.get_address_from_log_line(log_line)
    rider['address_id'] = load.add_address(address)
    load.add_rider(rider)
    rider['min_heart_rate'] = validate_heart_rate.calculate_min_heart_rate(rider)
    rider['max_heart_rate'] = validate_heart_rate.calculate_max_heart_rate(rider)
    return rider


def bike_pipeline(log_line: str):
    """
    Function to extract bike serial number from log line, upload it to the db (or locate it if it
    already exists), and return the bike_id number.
    """
    bike_serial_number = transform.get_bike_serial_number_from_log_line(log_line)
    return load.add_bike(bike_serial_number)


def ride_pipeline(log_line: str, bike_id: int) -> dict:
    """
    Function to extract ride info from the given log line, upload it to the db, and return the id
    of the ride.
    """
    ride_info = transform.get_ride_data_from_log_line(log_line)
    ride_info['bike_id'] = bike_id
    ride_info['ride_id'] = load.add_ride(ride_info)
    return ride_info


def reading_pipeline(log_line: str, ride_id: int, start_time: datetime, rider: dict,
                     consecutive_extreme_hrs: list) -> dict:
    """
    Function to extract reading data from log_line, add it to reading dict, and (for every pair of
    readings) upload to db and alert rider by email if their heart rate has had an extreme value
    for enough consecutive readings.
    """
    reading = {'ride_id': ride_id}
    reading = transform.get_reading_data_from_log_line(reading, log_line, start_time)
    if 'heart_rate' in reading:
        # Heart rate comes with the second of every pair of reading log lines.
        load.add_reading(reading)

        if (reading['heart_rate'] == 0) or \
            (rider['min_heart_rate'] <= reading['heart_rate'] <= rider['max_heart_rate']):
            consecutive_extreme_hrs.clear()
        else:
            consecutive_extreme_hrs.append(reading['heart_rate'])

        if len(consecutive_extreme_hrs) == EXTREME_HR_COUNT_THRESHOLD:
            try:
                validate_heart_rate.send_email(rider, consecutive_extreme_hrs)
            except ClientError:
                print('Unable to send email.')
            consecutive_extreme_hrs.clear()
        
    return reading


def live_pipeline():
    """
    Function to run the main pipeline; establishes connection to Kafka stream, retrieves messages,
    utilises transform module to get data, and uses load module to upload to the db.
    """
    kafka_consumer = get_kafka_consumer(GROUP_ID)
    rider = None
    messages = []

    while True:
        log_line = get_next_log_line(kafka_consumer, messages)
        if '[SYSTEM]' in log_line:
            consecutive_extreme_hrs = []
            rider = rider_pipeline(log_line)
            bike_id = bike_pipeline(log_line)
            ride = ride_pipeline(log_line, bike_id)

        if ('[INFO]' in log_line) and rider:
            reading_pipeline(
                log_line, ride['ride_id'], ride['start_time'], rider, consecutive_extreme_hrs)


def pipeline():
    """
    Runs first the backfill_pipeline, and then the live_pipeline, until fatal error or user
    interrupt.
    """
    logging.INFO("Running backfill_pipeline...")
    backfill_pipeline.backfill_pipeline()
    logging.INFO("backfill_pipeline finished.")
    logging.INFO("Running live_pipeline...")
    live_pipeline()


if __name__ == "__main__":
    load_dotenv()
    pipeline()
