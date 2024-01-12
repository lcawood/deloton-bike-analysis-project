"""
Module containing functions that act as a go between between functions in database_functions.py and
the endpoints in api.py; this allows for better compartmentalisation of code, and easier testing of
functionality.
"""

from datetime import datetime
from psycopg2.extensions import connection
from psycopg2.errors import Error

import database_functions
from api_helper_functions import format_seconds_as_readable_time, is_positive_integer, \
    is_string_boolean


STATUS_CODES = {
    'success': 200,
    'bad request': 400,
    'not found': 404,
    'server error': 500
}

ERROR_MESSAGES = {
    'bad request': {
        'id': 'Invalid url; {} must be a positive integer.',
        'boolean': 'Invalid url; {} must be a boolean value (True/False).',
        'datetime': 'Invalid url; {} must be a datetime string matching the format dd-mm-yyyy.'
    },
    'not found': {
        'ride': 'Ride with id {} could not be found.',
        'rider': 'Rider with id {} could not be found.',
        'rider_rides': 'Unable to locate any rides belonging to a rider with id {}.',
        'daily_rides': 'Unable to locate any rides starting on {}.'
    },
    'server error': 'Oops! There has been a problem on our end; \
please be patient while we reset our database connection (if this problem persists, please \
contact IT support).'
}


def get_ride(db_conn: connection, ride_id: int, expanded: str = 'False',
             summary: str = 'False') -> (dict, int):
    """
    Function to attempt to retrieve ride with specified id from database using relevant
    database_functions, returning a dictionary of said ride with status code 200 if successful,
    and an error dict with appropriate status code if not.
    """
    if not is_positive_integer(ride_id):
        return {'error': {
            'code': STATUS_CODES['bad request'],
            'type': 'bad request'
            },
            'message': ERROR_MESSAGES['bad request']['id'].format('ride_id')}, \
            STATUS_CODES['bad request']

    for name, value in [('expanded', expanded), ('summary', summary)]:
        if not is_string_boolean(value):
            return {'error': {
                'code': STATUS_CODES['bad request'],
                'type': 'bad request'
            },
            'message': ERROR_MESSAGES['bad request']['boolean'].format(name)}, \
                STATUS_CODES['bad request']

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

    except Error:
        return {'error': {
            'code': STATUS_CODES['server error'],
            'type': 'server error'
            },
            'message': ERROR_MESSAGES['server error']
        }, STATUS_CODES['server error']

    if ride:
        return ride, STATUS_CODES['success']

    return {'error': {
        'code': STATUS_CODES['not found'],
        'type': 'not found'
        },
        'message': ERROR_MESSAGES['not found']['ride'].format(ride_id)}, STATUS_CODES['not found']


def get_rider(db_conn: connection, rider_id: int) -> (dict, int):
    """
    Function to attempt to retrieve rider with specified id from database using relevant
    database_functions, returning a dictionary of said rider's information with status code 200 if
    successful, and an error dict with appropriate status code if not.
    """
    if not is_positive_integer(rider_id):
        return {'error': {
            'code': STATUS_CODES['bad request'],
            'type': 'bad request'
            },
            'message': ERROR_MESSAGES['bad request']['id'].format('rider_id')}, \
            STATUS_CODES['bad request']

    try:
        rider = database_functions.get_rider_by_id(db_conn, rider_id)
    except Error:
        return {'error': {
            'code': STATUS_CODES['server error'],
            'type': 'server error'
            },
            'message': ERROR_MESSAGES['server error']
        }, STATUS_CODES['server error']

    if rider:
        return rider, STATUS_CODES['success']

    return {'error': {
        'code': STATUS_CODES['not found'],
        'type': 'not found'
        },
        'message': ERROR_MESSAGES['not found']['rider'].format(rider_id)}, \
            STATUS_CODES['not found']


def get_rider_rides(db_conn: connection, rider_id: int, expanded: str = 'False',
                    summary: str = 'False') -> (dict, int):
    """
    Function to attempt to retrieve a list of all rides belonging to a rider with specified id from
    database using relevant database_functions, returning a list of dictionaries of said rider's
    rides with status code 200 if successful, and an error dict with appropriate status code if
    not.
    """
    if not is_positive_integer(rider_id):
        return {'error': {
            'code': STATUS_CODES['bad request'],
            'type': 'bad request'
        },
        'message': ERROR_MESSAGES['bad request']['id'].format('rider_id')}, \
            STATUS_CODES['bad request']

    for name, value in [('expanded', expanded), ('summary', summary)]:
        if not is_string_boolean(value):
            return {'error': {
                'code': STATUS_CODES['bad request'],
                'type': 'bad request'
                },
                'message': ERROR_MESSAGES['bad request']['boolean'].format(name)
                }, STATUS_CODES['bad request']

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

    except Error:
        return {'error': {
            'code': STATUS_CODES['server error'],
            'type': 'server error'
            },
            'message': ERROR_MESSAGES['server error']
        }, STATUS_CODES['server error']

    if rides:
        return rides, STATUS_CODES['success']

    return {'error': {
        'code': STATUS_CODES['not found'],
        'type': 'not found'
        },
        'message': ERROR_MESSAGES['not found']['rider_rides'].format(rider_id)}, \
        STATUS_CODES['not found']


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
        return {'error': {
            'code': STATUS_CODES['bad request'],
            'type': 'bad request'
            },
            'message':  ERROR_MESSAGES['bad request']['datetime'].format('date')
            }, STATUS_CODES['bad request']

    for name, value in [('expanded', expanded), ('summary', summary)]:
        if not is_string_boolean(value):
            return {'error': {
                'code': STATUS_CODES['bad request'],
                'type': 'bad request'
                },
                'message': ERROR_MESSAGES['bad request']['boolean'].format(name)}, \
                STATUS_CODES['bad request']

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

    except Error:
        return {'error': {
            'code': STATUS_CODES['server error'],
            'type': 'server error'
            },
            'message': ERROR_MESSAGES['server error']
        }, STATUS_CODES['server error']

    if rides:
        return rides, STATUS_CODES['success']

    return {'error': {
        'code': STATUS_CODES['not found'],
        'type': 'not found'
        },
        'message': ERROR_MESSAGES['not found']['daily_rides'].format(date.strftime("%d-%m-%Y"))
        }, STATUS_CODES['not found']


def delete_ride(db_conn: connection, ride_id: int) -> (dict, int):
    """
    Function to attempt to delete ride with specified id from database using relevant
    database_functions, returning a dictionary of said ride with status code 200 if successful,
    and an error dict with appropriate status code if not.
    """
    if not is_positive_integer(ride_id):
        return {'error': {
            'code': STATUS_CODES['bad request'],
            'type': 'bad request'
            },
            'message': ERROR_MESSAGES['bad request']['id'].format('ride_id')}, \
            STATUS_CODES['bad request']

    try:
        ride = database_functions.delete_ride_by_id(db_conn, ride_id)
    except Error:
        return {'error': {
            'code': STATUS_CODES['server error'],
            'type': 'server error'
            },
            'message': ERROR_MESSAGES['server error']
        }, STATUS_CODES['server error']

    if ride:
        return ride, STATUS_CODES['success']

    return {'error': {
        'code': STATUS_CODES['not found'],
        'type': 'not found'
        },
        'message': ERROR_MESSAGES['not found']['ride'].format(ride_id)}, STATUS_CODES['not found']
