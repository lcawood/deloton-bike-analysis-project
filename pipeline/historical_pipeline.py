"""
Implementation of the pipeline script that batches ride data and utilises specialised functions
with multiprocessing to speed up processing; no heart rate validation functionality.

Can be run to a certain date, and will the save the first [SYSTEM] line of that date in the s3
bucket for the regular pipeline to read from to continue.
"""

import argparse
from datetime import datetime
from functools import partial
import logging
from multiprocessing import Pool, Process
import os

from dotenv import load_dotenv
import pandas as pd

import load
import transform
import pipeline


GROUP_ID = "pipeline_zeta"
FIRST_READING_TIME = datetime.strptime("2023-10-05 22:09:48", '%Y-%m-%d %H:%M:%S')


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
    Function to extract reading data from a pair of reading_lines (concatenated), and return as dict.
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


def format_seconds_as_readable_time(seconds: int) -> str:
    """
    Takes in an integer number of seconds, and returns a human readable string of the time in
    hours, minutes and seconds.
    """

    hours = seconds // (60 ** 2)
    seconds -= hours * (60 ** 2)
    minutes = seconds // 60
    seconds -= minutes * 60

    time_string = f"{seconds}s"

    if minutes:
        time_string = f'{minutes}m ' + time_string

    if hours:
        time_string = f'{hours}h ' + time_string

    return time_string


def historical_pipeline(str_stop_date: str):
    """
    Function to run the main pipeline; establishes connection to Kafka stream, retrieves messages,
    utilises transform module to get data, and uses load module to upload to the db (in batches
    with multiprocessing).
    """

    kafka_consumer = pipeline.get_kafka_consumer(GROUP_ID)
    messages = []
    log_line = ""
    p = None

    first_reading_time = None

    try:
        if os.path.exists('readings.csv'):
            load.add_readings_from_csv('readings.csv')
        
        start = int(datetime.now().strftime('%s'))
        while True:

            reading_log_lines = []
            if '[SYSTEM]' in log_line:

                if "2024-01-06 22:" in log_line:
                    return None

                rider_pipeline(log_line)
                bike_serial_number = transform.get_bike_serial_number_from_log_line(
                    log_line)
                bike_id = load.add_bike(bike_serial_number)
                ride = pipeline.ride_pipeline(log_line, bike_id)

                log_line = pipeline.get_next_log_line(kafka_consumer, messages)

                while '[INFO]' in log_line:
                    log_line += pipeline.get_next_log_line(kafka_consumer, messages)
                    reading_log_lines.append(log_line)
                    log_line = pipeline.get_next_log_line(kafka_consumer, messages)

                if p:
                    p.join()
                    p.close()

                    ride_st_seconds = int(ride['start_time'].strftime('%s'))
                    now = int(datetime.now().strftime('%s'))
                    print()
                    print("Start time of ride being processed: ", ride['start_time'])
                    print("  Estimated time remaining: ",
                          format_seconds_as_readable_time(int(
                              (now - ride_st_seconds)*(
                                  (now - start)/(
                                      ride_st_seconds - first_reading_time)))))

                if not first_reading_time:
                    first_reading_time = int(ride['start_time'].strftime('%s'))

                p = Process(target=process_readings,
                            args=(reading_log_lines, ride['ride_id'], ride['start_time']))
                p.start()

                reading_log_lines.clear()

            else:
                log_line = pipeline.get_next_log_line(kafka_consumer, messages)

    except BaseException as e:
        print('Two secs...')
        with open('historical_pipeline_log.txt', 'a') as file:
            error_log_string = f"\n\n\n *** Error on log line: {log_line}\n\n    " + repr(e) + "\n    " + str(e)
            file.write(error_log_string)
        raise e


if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser()

    parser.add_argument("--stop_date", "-d", default=datetime.today().strftime('%Y-%m-%d'),
                        help="Optional argument for the timestamp date on which to stop the \
                            pipeline; defaults to current day.")
    
    args = parser.parse_args()

    logging.basicConfig(filename=LOG_FILE, filemode='a', encoding='utf-8',
                        level=logging.ERROR)

    try:
        datetime.strptime(args.stop_date, '%Y-%m-%d')
    except ValueError as e:
        print("Invalid stop date; must be date string of format yyyy-mm-dd.")
        raise e
    
    historical_pipeline(args.stop_date)
