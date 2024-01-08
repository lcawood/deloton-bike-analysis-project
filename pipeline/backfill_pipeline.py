"""
Implementation of the pipeline script that batches ride data and utilises specialised functions
with multiprocessing to speed up processing; no heart rate validation functionality.

Can be run to a certain date, and will the save the first [SYSTEM] line of that date in the s3
bucket for the regular pipeline to read from to continue; by default runs to the current hour in
which the program is being run.
"""

import argparse
from datetime import datetime
from functools import partial
from multiprocessing import Pool, Process
import os

from dotenv import load_dotenv
import pandas as pd

import load
import transform
import pipeline


GROUP_ID = "pipeline_zeta"
FIRST_READING_TIME = datetime.strptime("2023-10-05 22:09:48", '%Y-%m-%d %H:%M:%S')
LOG_FILE = "historical_pipeline.log.txt"


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


def reading_pipeline(reading_line_pair: str, ride_id: int, start_time: datetime):
    """
    Function to extract reading data from a pair of reading_lines (concatenated), and return as
    dict.
    """
    reading = {'ride_id': ride_id}
    for reading_line in reading_line_pair.split('\n'):
        reading = transform.get_reading_data_from_log_line(reading, reading_line, start_time)
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


def backfill_pipeline(custom_stop_date: datetime = None):
    """
    Function to run the main pipeline; establishes connection to Kafka stream, retrieves messages,
    utilises transform module to get data, and uses load module to upload to the db (in batches
    with multiprocessing).
    """

    kafka_consumer = pipeline.get_kafka_consumer(GROUP_ID)
    messages = []
    log_line = ""
    p = None

    if os.path.exists('readings.csv'):
        load.add_readings_from_csv('readings.csv')

    while True:

        reading_log_lines = []
        if '[SYSTEM]' in log_line:

            rider_pipeline(log_line)
            bike_id = pipeline.bike_pipeline(log_line)
            ride = pipeline.ride_pipeline(log_line, bike_id)

            # Stops historical_pipeline automatically if caught up to specified datetime.
            if stop_date:
                if (stop_date < ride['start_time']):
                    return None
            elif datetime.now().strftime('%Y-%m-%d %H') in log_line:
                return None

            # Concatenates each pair of [INFO] log lines and adds to list
            # (until [SYSTEM] log line is hit).
            log_line = pipeline.get_next_log_line(kafka_consumer, messages)
            while '[INFO]' in log_line:
                log_line += pipeline.get_next_log_line(kafka_consumer, messages)
                reading_log_lines.append(log_line)
                log_line = pipeline.get_next_log_line(kafka_consumer, messages)

            # Each batch of reading uploads to db use the same .csv file, so each must be allowed
            # to finish before the next begins.
            if p:
                p.join()
                p.close()

                print("Start time of ride last processed: ", ride['start_time'])

            # Multiprocessing used to cycle around and retrieve and transform the next ride's Kafka
            # messages while the last batch of readings are being uploaded.
            p = Process(target=process_readings,
                        args=(reading_log_lines, ride['ride_id'], ride['start_time']))
            p.start()

            reading_log_lines.clear()

        else:
            # Skips over lines which contain neither [SYSTEM] nor [INFO] data.
            log_line = pipeline.get_next_log_line(kafka_consumer, messages)


if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser()

    parser.add_argument("--stop_date", "-d",
                        help="Optional argument for the timestamp date on which to stop the " + \
                        "pipeline; defaults to the current date and time to the resolution of " + \
                        "the hour (increasing as the program runs).")

    args = parser.parse_args()
    
    stop_date = args.stop_date

    if stop_date:
        try:
            stop_date = datetime.strptime(stop_date, '%Y-%m-%d %H'[:len(stop_date) - 2])
        except ValueError as e:
            print("Invalid stop date; must be a datetime substring (starting at the same index) of " +
                  "the format 'yyyy-mm-dd hh'; for example '2024', '2024-01-05', and " +
                  "'2024-01-05 12' are all valid options.")
            raise e

    backfill_pipeline(stop_date)
