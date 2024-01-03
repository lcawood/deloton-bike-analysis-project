"""
Pipeline script to establish connection to Kafka stream, retrieve log lines, and then transform and
upload the relevant data to the database (using functions from the `transform.py` and `load.py`
files as necessary).

User's max and min heart rates are calculated using functions from validate_heart_rate, and their
heart rate given in the readings compared against them; if it above or below the healthy range too
many times in a row (EXTREME_HR_COUNT_THRESHOLD), the validate_heart_rate function send_email is
used to alert the user.
"""

import json
import logging
from os import environ

from confluent_kafka import Consumer, KafkaException, Message
from dotenv import load_dotenv

import load
import transform
import validate_heart_rate


load_dotenv()
GROUP_ID = "testing5"
EXTREME_HR_COUNT_THRESHOLD = 3



def get_kafka_consumer(group_id: str) -> Consumer:
    """Function to return a consumer for the kafka cluster specified in .env"""
    try:
        kafka_config = {
            'bootstrap.servers': environ['BOOTSTRAP_SERVERS'],
            'security.protocol': environ['SECURITY_PROTOCOL'],
            'sasl.mechanisms': environ['SASL_MECHANISM'],
            'sasl.username': environ['USERNAME'],
            'sasl.password': environ['PASSWORD'],
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        }
        consumer = Consumer(kafka_config)
        consumer.subscribe([environ['KAFKA_TOPIC']])
        return consumer

    except KafkaException as error:
        logging.critical("Unable to connect to Kafka stream.")
        raise error


def get_next_log_line(consumer: Consumer) -> str:
    """Function to retrieve and return next log line from kafka stream."""
    message = None
    while (not message) or ('log' not in message):
        message = consumer.poll(1)
        if isinstance(message, Message):
            message = json.loads(message.value().decode())

    return message.get('log')


def user_pipeline(log_line: str) -> dict:
    """
    Function to extract user and address information from log_line, add it to the db, and return
    user dictionary (with max and min heart rate fields).
    """
    user = transform.get_user_from_log_line(log_line)
    address = transform.get_address_from_log_line(log_line)
    user['address_id'] = load.add_address(address)
    load.add_user(user)

    user['min_heart_rate'] = validate_heart_rate.calculate_min_heart_rate(user)
    user['max_heart_rate'] = validate_heart_rate.calculate_max_heart_rate(user)

    return user


def ride_pipeline(log_line: str, bike_id: int) -> int:
    """
    Function to extract ride info from the given log line, upload it to the db, and return the id
    of the ride.
    """
    ride_info = transform.get_ride_data_from_log_line(log_line)
    ride_info['bike_id'] = bike_id

    return load.add_ride(ride_info)


def reading_pipeline(log_line: str, ride_id: int, reading: dict, user: dict,
                     consecutive_extreme_hrs: list) -> dict:
    """
    Function to extract reading data from log_line, add it to reading dict, and (for every pair of
    readings) upload to db and alert user by email if their heart rate has had an extreme value
    for enough consecutive readings.
    """
    reading = transform.get_reading_data_from_log_line(reading, log_line)

    if 'heart_rate' in reading:
        # Heart rate comes with the second of every pair of reading log lines.
        if user['min_heart_rate'] <= reading['heart_rate'] <= user['max_heart_rate']:
            consecutive_extreme_hrs.clear()
        else:
            consecutive_extreme_hrs.append(reading['heart_rate'])

        if len(consecutive_extreme_hrs) == EXTREME_HR_COUNT_THRESHOLD:
            validate_heart_rate.send_email(user, consecutive_extreme_hrs)
            consecutive_extreme_hrs.clear()

        load.add_reading(reading)
        reading.clear()
        reading['ride_id'] = ride_id

    return reading


def pipeline():
    """
    Function to run the main pipeline; establishes connection to Kafka stream, retrieves messages,
    utilises transform module to get data, and uses load module to upload to the db.
    """
    kafka_consumer = get_kafka_consumer(GROUP_ID)

    new_ride = False
    while True:
        log_line = get_next_log_line(kafka_consumer)

        if "beginning of a new ride" in log_line:
            new_ride = True

        elif ('[SYSTEM]' in log_line) and new_ride:
            user = user_pipeline(log_line)
            consecutive_extreme_hrs = []

            bike_serial_number = transform.get_bike_serial_number_from_log_line(log_line)
            bike_id = load.add_bike(bike_serial_number)

            ride_id = ride_pipeline(log_line, bike_id)

            reading = {'ride_id': ride_id}
            new_ride = False

        elif ('[INFO]' in log_line) and (not new_ride):
            reading = reading_pipeline(log_line, ride_id, reading, user, consecutive_extreme_hrs)



if __name__ == "__main__":
    pipeline()
