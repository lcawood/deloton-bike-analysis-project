'''Module to transform the data received from the Kafka cluster.'''

from ast import literal_eval
from datetime import datetime, timedelta
import re

import spacy

INVALID_DATE_THRESHOLD = datetime(1900, 1, 1, 0, 0, 0)


def timestamp_to_date(timestamp_ms: int) -> str:
    '''Helper function that converts a timestamp in milliseconds
    since the Unix epoch to a date string in the form YYYY-MM-DD.'''

    return datetime.utcfromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d')


def check_datetime_is_valid(dt: datetime) -> bool:
    '''Helper function that returns True if a datetime is valid, i.e. each component
    in the datetime is within its appropriate range, or returns False if invalid.'''

    if dt > datetime.now() or dt < INVALID_DATE_THRESHOLD:
        return False
    return True


def extract_datetime_from_string(input_string: str) -> datetime | None:
    '''Helper function to extract a datetime object from a string that contains
    a datetime in the format 'YYYY-MM-DD HH:MM:SS.microseconds'.'''

    pattern = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+'
    match = re.search(pattern, input_string)
    try:
        datetime_obj = datetime.strptime(
            match.group(), '%Y-%m-%d %H:%M:%S.%f')
        if check_datetime_is_valid(datetime_obj):
            return datetime_obj
    except (ValueError, AttributeError):
        return None
    return None


def get_bike_serial_number_from_log_line(log_line: str) -> str | None:
    '''Takes in a kafka log line, and returns the bike serial number if found.'''

    log_line_dict = literal_eval(log_line.split('=')[1])
    if 'bike_serial' in log_line_dict.keys():
        return log_line_dict['bike_serial']
    return None


def get_email_from_log_line(log_line: str) -> str | None:
    '''Helper function to extract an email address from a log line using regex if found.'''

    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    match = re.search(email_pattern, log_line)
    if match:
        return match.group()
    return None


def get_user_from_log_line(log_line: str) -> dict:
    '''Takes in a kafka log line and returns a dictionary of user data from it (excluding address).
    If any user information is missing, this field is given as None in the returned dictionary.'''

    user = {}
    log_line_data = literal_eval(log_line.split('=')[1])

    # Obtain user data from the log line directly
    try:
        user['user_id'] = int(log_line_data['user_id'])
    except KeyError:
        user['user_id'] = None
    try:
        user['first_name'] = log_line_data['name'].split()[0]
    except KeyError:
        user['first_name'] = None
    try:
        user['last_name'] = " ".join(log_line_data['name'].split()[1:])
    except KeyError:
        user['last_name'] = None
    try:
        user_dob = log_line_data['date_of_birth']
        user['birthdate'] = timestamp_to_date(user_dob)
    except KeyError:
        user['birthdate'] = None
    try:
        user['height'] = int(log_line_data['height_cm'])
    except KeyError:
        user['height'] = None
    try:
        user['weight'] = int(log_line_data['weight_kg'])
    except KeyError:
        user['weight'] = None
    # Email key-pair is added if found, otherwise is given as None
    user['email'] = get_email_from_log_line(log_line)
    try:
        user['gender'] = log_line_data['gender']
    except KeyError:
        user['gender'] = None
    try:
        account_created_date = log_line_data['account_create_date']
        user['account_created'] = timestamp_to_date(account_created_date)
    except KeyError:
        user['account_created'] = None

    return user


def get_ride_data_from_log_line(log_line: str) -> dict:
    '''
    Takes in a kafka log line and returns a dictionary of ride data from it (corresponding to
    non-auto-generated attributes in ride table in db). If a given field is not found, its value
    in the returned dictionary is given as None.
    '''

    ride = {}

    log_line_data = literal_eval(log_line.split('=')[1])

    try:
        ride['user_id'] = int(log_line_data['user_id'])
    except KeyError:
        ride['user_id'] = None

    if extract_datetime_from_string(log_line):
        ride['start_time'] = (extract_datetime_from_string(
            log_line) - timedelta(seconds=0.5)).strftime('%Y-%m-%d %H:%M:%S')
    else:
        ride['start_time'] = None

    return ride


def get_reading_data_from_log_line(reading: dict, log_line: str, start_time: datetime) -> dict:
    '''
    Takes in a kafka log line, and transforms and appends reading data
    contained within it to the given reading dictionary.
    '''
    if 'Ride' in log_line:
        try:
            reading['resistance'] = int(
                log_line.split(';')[-1].split('=')[1].strip())
        except IndexError:
            reading['resistance'] = None

        if extract_datetime_from_string(log_line) is not None and \
                extract_datetime_from_string(log_line) > start_time:
            reading['elapsed_time'] = int((extract_datetime_from_string(
                log_line) - start_time).total_seconds())
        else:
            reading['elapsed_time'] = None

    elif 'Telemetry' in log_line:
        reading['heart_rate'] = int(
            log_line.split(';')[0].split('=')[1].strip())
        reading['power'] = float(log_line.split('=')[-1].strip())
        reading['rpm'] = int(log_line.split(';')[1].split('=')[1].strip())

    return reading


def get_address_from_log_line(log_line: str) -> dict:
    '''Takes in a kafka log line and returns a dictionary of address data from it.'''

    address = {}
    address_dict = literal_eval(log_line.split('=')[1])
    if 'address' not in address_dict.keys():
        address['first_line'] = None
        address['second_line'] = None
        address['city'] = None
        address['postcode'] = None
        return address

    address_string = address_dict['address']

    # Assumption: the first line of the address is everything before the first comma
    if ',' in address_string:
        first_line = address_string.split(',')[0]
        address['first_line'] = first_line
    else:
        first_line = None

    # Assumption: city is last in order of found named entities
    nlp = spacy.load('en_core_web_md')
    doc = nlp(address_string)
    city = [ent.text for ent in doc.ents if ent.label_ == 'GPE']
    if city:
        address['city'] = city[-1]
    else:
        address['city'] = None

    # Assumption: users live in the UK only
    uk_postcode_pattern = r'[A-Za-z]{1,2}\d[A-Za-z\d]?\s*\d[A-Za-z]{2}'
    postcode = re.search(uk_postcode_pattern, address_string)
    if postcode:
        address['postcode'] = postcode.group()
    else:
        address['postcode'] = None

    # Assumption: the second line of the address is everything else not found
    if address_string:
        address_string = address_string.replace(first_line, '')
    if city:
        address_string = address_string.replace(city[-1], '')
    if postcode:
        address_string = address_string.replace(postcode.group(), '')

    second_line = address_string.replace(',', '').strip()
    if second_line:
        address['second_line'] = second_line
    else:
        address['second_line'] = None

    return address


if __name__ == "__main__":

    pass
