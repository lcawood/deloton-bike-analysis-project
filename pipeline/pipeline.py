"""Pipeline script"""

from os import environ
import json

from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv

import transform
import load
import validate_heart_rate


load_dotenv()
GROUP_ID = "testing"
EXTREME_HR_COUNT_THRESHOLD = 3



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
            reading_count = 0

        elif ('[SYSTEM]' in log_line) and new_ride:
            user = transform.get_user_from_log_line(log_line)
            address = transform.get_address_from_log_line(log_line)
            user['address_id'] = load.add_address(address)
            load.add_user(user)

            user['max_heart_rate'] = validate_heart_rate.calculate_max_heart_rate(user)
            user['min_heart_rate'] = validate_heart_rate.calculate_min_heart_rate(user)
            consecutive_extreme_hrs = []

            bike_serial_number = transform.get_bike_serial_number_from_log_line(log_line)
            bike_id = load.add_bike(bike_serial_number)

            ride_info = transform.get_ride_data_from_log_line(log_line)
            ride_info['bike'] = bike_id

            ride_id = load.add_ride(ride_info)
            reading = {'ride_id': ride_id}
            new_ride = False
        
        elif ('[INFO]' in log_line) and (not new_ride):
            reading = transform.get_reading_data_from_log_line(reading, log_line)
            reading_count = (reading_count + 1) % 2  # Readings come in pairs

            if reading_count == 0:
                if user['min_heart_rate'] <= reading['heart_rate'] <= user['max_heart_rate']:
                    consecutive_extreme_hrs = []
                else:
                    consecutive_extreme_hrs.append(reading['heart_rate'])

                if len(consecutive_extreme_hrs) % EXTREME_HR_COUNT_THRESHOLD == 0:
                    validate_heart_rate.send_email(user, consecutive_extreme_hrs)
                    consecutive_extreme_hrs = 0

                load.add_reading(reading)
                reading = {'ride_id': ride_id}


if __name__ == "__main__":
    pipeline()
