"""
Implementation of the pipeline script that batches ride data and utilises specialised functions
with multiprocessing to speed up processing; no heart rate validation functionality.

Can be run to a certain date, and will the save the first [SYSTEM] line of that date in the s3
bucket for the regular pipeline to read from to continue.
"""

import argparse
from datetime import datetime
from functools import partial
from multiprocessing import Pool, Process

from dotenv import load_dotenv
import pandas as pd

import load
import transform
import pipeline


GROUP_ID = "pipeline_10"


def rider_pipeline(log_line: str) -> dict:
    """
    Function to extract rider and address information from log_line, add it to the db, and return
    rider dictionary (without max and min heart rate fields).
    """
    rider = transform.get_rider_from_log_line(log_line)
    address = transform.get_address_from_log_line(log_line)
    rider['address_id'] = load.add_address(address)
    load.add_rider(rider)
    return rider


def reading_pipeline(log_line_pair: str, ride_id: int, start_time: datetime):
    """
    Function to extract reading data from a pair of log_lines (concatenated), and return as dict.
    """
    reading = {'ride_id': ride_id}
    for log_line in log_line_pair.split('\n'):
        reading = transform.get_reading_data_from_log_line(reading, log_line, start_time)
    return reading


def process_readings(reading_log_lines: list[str], ride_id: int, start_time: datetime):
    """
    Function to process a list of reading log_line strings (where each string is two halves of a
    reading concatenated).
    """
    partial_reading_pipeline = partial(
        reading_pipeline, ride_id = ride_id, start_time = start_time)
    
    with Pool() as pool:
        readings = pd.DataFrame(pool.map(partial_reading_pipeline, reading_log_lines))

    readings.to_csv("readings.csv", index=False)
    load.add_readings_from_csv("readings.csv")

    print(start_time)


def historical_pipeline(str_stop_date: str):
    """
    Function to run the main pipeline; establishes connection to Kafka stream, retrieves messages,
    utilises transform module to get data, and uses load module to upload to the db (in batches
    with multiprocessing).
    """

    kafka_consumer = pipeline.get_kafka_consumer(GROUP_ID)
    first_relevant_line = True
    readings = pd.DataFrame()
    log_line = pipeline.get_next_log_line(kafka_consumer)
    while True:
        reading_log_lines = []
        if ('[SYSTEM]' in log_line) or (('[INFO]: Ride' in log_line) and first_relevant_line):
            # SYSTEM log_line, or the pipeline is starting mid ride.

            if '[INFO]' in log_line:
                system_log_line = pipeline.retrieve_text_from_s3_file()
            else:
                system_log_line = log_line
                pipeline.save_log_line_to_s3(system_log_line)

                if str_stop_date in system_log_line:
                    return None

            if system_log_line:
                rider_pipeline(system_log_line)
                bike_serial_number = transform.get_bike_serial_number_from_log_line(
                    system_log_line)
                bike_id = load.add_bike(bike_serial_number)
                ride = pipeline.ride_pipeline(system_log_line, bike_id)

                if '[SYSTEM]' in log_line:
                    log_line = pipeline.get_next_log_line(kafka_consumer)

                try:
                    while '[INFO]' in log_line:
                        log_line += pipeline.get_next_log_line(kafka_consumer)
                        reading_log_lines.append(log_line)
                        log_line = pipeline.get_next_log_line(kafka_consumer)

                    p = Process(target=process_readings, args=(reading_log_lines, ride['ride_id'], ride['start_time']))
                    p.start()

                except BaseException as e:
                    # Saves readings already received from kafka stream to db before program ends.
                    print("Two secs....")
                    process_readings(reading_log_lines, ride['ride_id'], ride['start_time'])
                    print("Data saved; program end.")
                    raise e

                reading_log_lines.clear()

            first_relevant_line = False

        else:
            log_line = pipeline.get_next_log_line(kafka_consumer)


if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser()

    parser.add_argument("--stop_date", "-d", default=datetime.today().strftime('%Y-%m-%d'),
                        help="Optional argument for the timestamp date on which to stop the pipeline; defaults to current day.")
    
    args = parser.parse_args()

    try:
        datetime.strptime(args.stop_date, '%Y-%m-%d')
        historical_pipeline(args.stop_date)
    except ValueError:
        print("Invalid stop date; must be date string of format yyyy-mm-dd.")


