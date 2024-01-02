"""Pipeline script"""

from os import environ
import json

from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv

import transform
import load
import database_functions


load_dotenv()
GROUP_ID = "testing"



def get_kafka_consumer(group_id: str, topic: str = environ['KAFKA_TOPIC']) -> Consumer:
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
        consumer.subscribe([topic])
        return consumer

    except KafkaException as e:
        raise e


def pipeline():
    """
    Function to run the main pipeline; establishes connection to Kafka stream, retrieves messages,
    utilises transform module to get data, and uses load module to upload to the db.
    """
    kafka_consumer = get_kafka_consumer(GROUP_ID)
    
    new_ride = False
    while True:
        message = None
        while not message:
            message = kafka_consumer.poll(1)
        log_line = json.loads(message.value().decode()).get('log')

        if "beginning of a new ride" in log_line:
            new_ride = True
            count = 0

        elif ('[SYSTEM]' in log_line) and new_ride:
            user = transform.get_user_from_log_line(log_line)
            if not database_functions.get_user_by_id(user['user_id']):
                load.add_user(user)

            ride_info = transform.get_ride_data_from_log_line(log_line)
            ride_id = load.add_ride(ride_info)
            reading = {'ride_id': ride_id}
            new_ride = False
        
        elif ('[INFO]' in log_line) and (not new_ride):
            reading = transform.get_reading_data_from_log_line(reading, log_line)
            count = (count + 1) % 2  # Readings come in pairs
            if count == 0:
                load.add_reading(reading)
                reading = {'ride_id': ride_id}


if __name__ == "__main__":
    pipeline()
