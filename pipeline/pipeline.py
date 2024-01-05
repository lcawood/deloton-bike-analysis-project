"""
Pipeline script to establish connection to Kafka stream, retrieve log lines, and then transform and
upload the relevant data to the database (using functions from the `transform.py` and `load.py`
files as necessary).
User's max and min heart rates are calculated using functions from validate_heart_rate, and their
heart rate given in the readings compared against them; if it above or below the healthy range too
many times in a row (EXTREME_HR_COUNT_THRESHOLD), the validate_heart_rate function send_email is
used to alert the rider.
"""

from datetime import datetime
import json
import logging
from os import environ

from boto3 import client
from botocore.exceptions import ClientError

from confluent_kafka import Consumer, KafkaException, Message
from dotenv import load_dotenv
import load
import transform
import validate_heart_rate

GROUP_ID = "testing24"
EXTREME_HR_COUNT_THRESHOLD = 3
S3_BACKUP_FILENAME = "pipeline_backup.txt"

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


def ride_pipeline(log_line: str, bike_id: int) -> dict:
    """
    Function to extract ride info from the given log line, upload it to the db, and return the id
    of the ride.
    """
    ride_info = transform.get_ride_data_from_log_line(log_line)
    ride_info['bike_id'] = bike_id
    ride_info['ride_id'] = load.add_ride(ride_info)
    return ride_info


def reading_pipeline(log_line: str, ride_id: int, start_time: datetime, reading: dict, rider: dict,
                     consecutive_extreme_hrs: list) -> dict:
    """
    Function to extract reading data from log_line, add it to reading dict, and (for every pair of
    readings) upload to db and alert rider by email if their heart rate has had an extreme value
    for enough consecutive readings.
    """
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

        reading.clear()
        reading['ride_id'] = ride_id
    return reading


def get_s3_client():
    """Function to return boto3 s3 client; returns None if connection can't be made."""
    try:
        return client("s3",
                        aws_access_key_id=environ['AWS_ACCESS_KEY_ID_'],
                        aws_secret_access_key=environ['AWS_SECRET_ACCESS_KEY_'])
    except ClientError:
        return None


def save_log_line_to_s3(log_line: str, filename: str = S3_BACKUP_FILENAME):
    """
    Saves a log line to a text file in s3 bucket; allows the state of the pipeline to persist after
    a crash.
    """
    s3_client = get_s3_client()
    if s3_client is None:
        return None
    
    try:
        s3_client.put_object(Body = log_line, Bucket = environ['BUCKET_NAME'],
                             Key = filename)
    except ClientError:
        pass


def retrieve_text_from_s3_file(filename: str = S3_BACKUP_FILENAME):
    """Retrieves body of text file stores in s3_bucket."""
    s3_client = get_s3_client()
    if s3_client is None:
        return None
    
    try:
        return s3_client.get_object(
            Bucket=environ['BUCKET_NAME'], Key=filename)['Body'].read().decode("utf-8")
    except ClientError:
        return None


def pipeline():
    """
    Function to run the main pipeline; establishes connection to Kafka stream, retrieves messages,
    utilises transform module to get data, and uses load module to upload to the db.
    """
    kafka_consumer = get_kafka_consumer(GROUP_ID)
    rider = None
    first_relevant_line = True
    while True:
        log_line = get_next_log_line(kafka_consumer)

        if ('[SYSTEM]' in log_line) or (('[INFO]: Ride' in log_line) and first_relevant_line):
            # SYSTEM log_line, or the pipeline is starting mid ride.

            if '[INFO]' in log_line:
                system_log_line = retrieve_text_from_s3_file()
            else:
                system_log_line = log_line
                save_log_line_to_s3(system_log_line)

            if system_log_line:
                rider = rider_pipeline(system_log_line)
                consecutive_extreme_hrs = []
                bike_serial_number = transform.get_bike_serial_number_from_log_line(
                    system_log_line)
                bike_id = load.add_bike(bike_serial_number)
                ride = ride_pipeline(system_log_line, bike_id)
                reading = {'ride_id': ride['ride_id']}

            first_relevant_line = False

        if ('[INFO]' in log_line) and rider:
            reading = reading_pipeline(
                log_line, ride['ride_id'], ride['start_time'], reading, rider,
                consecutive_extreme_hrs)


if __name__ == "__main__":
    load_dotenv()
    pipeline()
