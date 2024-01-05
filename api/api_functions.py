"""
Module containing functions that act as a go between between functions in database_functions.py and
the endpoints in api.py; this allows for better compartmentalisation of code, and easier testing of
functionality.
"""

from datetime import datetime
from psycopg2.extensions import connection
from psycopg2.errors import Error

import database_functions
from api_helper_functions import format_seconds_as_readable_time


def get_ride(db_conn: connection, ride_id: int, expanded: str = 'False',
             summary: str = 'False') -> (dict, int):
    """
    Function to attempt to retrieve ride with specified id from database using relevant
    database_functions, returning a dictionary of said ride with status code 200 if successful,
    and an error dict with appropriate status code if not.
    """
    if (type(ride_id) != int) or (ride_id < 0):
        return {'error': 'Invalid url; ride_id must be a positive integer.'}, 400

    if expanded not in ['True', 'False']:
        return {'error': 'Invalid url; expanded must be a boolean value (True/False).'}, 400

    if summary not in ['True', 'False']:
        return {'error': 'Invalid url; summary must be a boolean value (True/False).'}, 400

    try:
        ride = database_functions.get_ride_by_id(db_conn, ride_id)

        if expanded == 'True':
            ride['readings'] = database_functions.get_readings_by_ride_id(
                db_conn, ride['ride_id'])

        if summary == 'True':
            ride['reading_summary'] = database_functions.get_readings_summary_for_ride_id(
                db_conn, ride['ride_id'])
            ride['reading_summary']['duration'] = format_seconds_as_readable_time(
                ride['reading_summary']['duration'])

    except Error as e:
        return {'error': str(e)}, 500

    if ride:
        return ride, 200

    return {'error': f'Ride with id {ride_id} could not be found.'}, 404


def get_rider(db_conn: connection, rider_id: int) -> (dict, int):
    """
    Function to attempt to retrieve rider with specified id from database using relevant
    database_functions, returning a dictionary of said rider's information with status code 200 if
    successful, and an error dict with appropriate status code if not.
    """
    if (type(rider_id) != int) or (rider_id < 0):
        return {'error': 'Invalid url; rider_id must be a positive integer.'}, 400

    try:
        rider = database_functions.get_rider_by_id(db_conn, rider_id)
    except Error as e:
        return {'error': str(e)}, 500

    if rider:
        return rider, 200

    return {'error': f'Rider with id {rider_id} could not be found.'}, 404


def get_rider_rides(db_conn: connection, rider_id: int, expanded: str = 'False',
                    summary: str = 'False') -> (dict, int):
    """
    Function to attempt to retrieve a list of all rides belonging to a rider with specified id from
    database using relevant database_functions, returning a list of dictionaries of said rider's
    rides with status code 200 if successful, and an error dict with appropriate status code if
    not.
    """
    if (type(rider_id) != int) or (rider_id < 0):
        return {'error': 'Invalid url; rider_id must be a positive integer.'}, 400

    if expanded not in ['True', 'False']:
        return {'error': 'Invalid url; expanded must be a boolean value (True/False).'}, 400

    if summary not in ['True', 'False']:
        return {'error': 'Invalid url; summary must be a boolean value (True/False).'}, 400

    try:
        rides = database_functions.get_rider_rides_by_id(db_conn, rider_id)

        if expanded == 'True':
            for i, ride in enumerate(rides):
                rides[i]['readings'] = database_functions.get_readings_by_ride_id(
                    db_conn, ride['ride_id'])

        if summary == 'True':
            for i, ride in enumerate(rides):
                rides[i]['reading_summary'] = database_functions.get_readings_summary_for_ride_id(
                    db_conn, ride['ride_id'])
                rides[i]['reading_summary']['duration'] = format_seconds_as_readable_time(
                    rides[i]['reading_summary']['duration'])

    except Error as e:
        return {'error': str(e)}, 500

    if rides:
        return rides, 200

    return {'error': f'Unable to locate any rides belonging to a rider with id {rider_id}.'}, 404


def get_daily_rides(db_conn: connection, date: str = datetime.today().strftime("%d-%m-%Y"),
                    expanded: str = 'False', summary: str = 'False') -> (dict, int):
    """
    Function to attempt to retrieve a list of all rides starting on a specified date (defaulting
    to the current date) from database using relevant database_functions, returning a list of
    dictionaries of said date's rides with status code 200 if successful, and an error dict with
    appropriate status code if not.
    """
    try:
        date = datetime.strptime(date, "%d-%m-%Y").date()
    except (TypeError, ValueError):
        return {
            'error': 'Invalid url; date must be a datetime string matching the format dd-mm-yyyy.'
            }, 400

    if expanded not in ['True', 'False']:
        return {'error': 'Invalid url; expanded must be a boolean value (True/False).'}, 400

    if summary not in ['True', 'False']:
        return {'error': 'Invalid url; summary must be a boolean value (True/False).'}, 400

    try:
        rides = database_functions.get_daily_rides(db_conn, date)

        if expanded == 'True':
            for i, ride in enumerate(rides):
                rides[i]['readings'] = database_functions.get_readings_by_ride_id(
                    db_conn, ride['ride_id'])

        if summary == 'True':
            for i, ride in enumerate(rides):
                rides[i]['reading_summary'] = database_functions.get_readings_summary_for_ride_id(
                    db_conn, ride['ride_id'])
                rides[i]['reading_summary']['duration'] = format_seconds_as_readable_time(
                    rides[i]['reading_summary']['duration'])

    except Error as e:
        return {'error': str(e)}, 500

    if rides:
        return rides, 200

    return {'error': f'Unable to locate any rides starting on {date.strftime("%d-%m-%Y")}.'}, 404


def delete_ride(db_conn: connection, ride_id: int) -> (dict, int):
    """
    Function to attempt to delete ride with specified id from database using relevant
    database_functions, returning a dictionary of said ride with status code 200 if successful,
    and an error dict with appropriate status code if not.
    """
    if (type(ride_id) != int) or (ride_id < 0):
        return {'error': 'Invalid url; ride_id must be a positive integer.'}, 400

    try:
        ride = database_functions.delete_ride_by_id(db_conn, ride_id)
    except Error as e:
        return {'error': str(e)}, 500

    if ride:
        return ride, 200

    return {'error': f'Ride with id {ride_id} could not be found.'}, 404
