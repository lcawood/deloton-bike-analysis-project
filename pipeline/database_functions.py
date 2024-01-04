"""Module containing functions used to interact with the RDS database."""

from os import environ

from dotenv import load_dotenv
import psycopg2
from psycopg2 import extensions

load_dotenv()

def get_database_connection() -> extensions.connection:
    """Return a connection our database."""

    return psycopg2.connect(user=environ["DATABASE_USERNAME"],
                            password=environ["DATABASE_PASSWORD"],
                            host=environ["DATABASE_IP"],
                            port=environ["DATABASE_PORT"],
                            database=environ["DATABASE_NAME"]
                            )


def load_address_into_database(db_connection : extensions.connection, address : dict) -> int:
    """Loads an Address from a dict into the database using SQL."""

    with db_connection.cursor() as db_cur:

        query = """INSERT INTO Address(first_line,second_line,city,postcode)
          VALUES (%s,%s,%s,%s) RETURNING address_id;"""

        parameters = (address["first_line"],address["second_line"],address["city"],
                        address["postcode"])

        db_cur.execute(query,parameters)

        address_id = db_cur.fetchone()

        db_connection.commit()

        return address_id[0]


def select_address_from_database(db_connection : extensions.connection, address : dict) -> int:
    """
    Selects a address id from the database using the address dict 
    passed in and a SQL Select Query.
    """

    with db_connection.cursor() as db_cur:

        query = """SELECT address_id FROM Address WHERE first_line=%s
            AND postcode=%s"""

        parameters = (address["first_line"],address["postcode"])

        db_cur.execute(query,parameters)

        address_id = db_cur.fetchone()

        db_connection.commit()

        return address_id[0]



def load_rider_into_database(db_connection : extensions.connection, rider : dict) -> int:
    """Loads a rider from the rider dict into the database using SQL."""

    with db_connection.cursor() as db_cur:

        query = """INSERT INTO Rider(rider_id,address_id,first_name,last_name,
        birthdate,height,weight,email,gender,account_created)
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING rider_id;"""

        parameters = (rider['rider_id'],rider['address_id'],rider['first_name'],
                        rider['last_name'],rider['birthdate'],rider['height'],
                        rider['weight'],rider['email'],rider['gender'],rider['account_created'])

        db_cur.execute(query,parameters)
        db_connection.commit()

        rider_id = db_cur.fetchone()

        return rider_id[0]


def load_ride_into_database(db_connection : extensions.connection, ride : dict) -> int:
    """Loads an Ride from the ride dict into the database using SQL."""

    with db_connection.cursor() as db_cur:

        query = """INSERT INTO Ride(rider_id,bike_id,start_time)
          VALUES (%s,%s,%s) RETURNING ride_id;"""
        
        parameters = (ride["rider_id"],ride["bike_id"],ride["start_time"])

        db_cur.execute(query,parameters)

        ride_id = db_cur.fetchone()

        db_connection.commit()

        return ride_id[0]


def select_ride_from_database(db_connection : extensions.connection, ride : dict) -> int:
    """
    Selects a ride id from the database using the ride dict 
    passed in and a SQL Select Query.
    """

    with db_connection.cursor() as db_cur:

        query = """SELECT ride_id FROM Ride WHERE rider_id=%s
            AND bike_id=%s AND start_time =%s"""

        parameters = (ride["rider_id"],ride["bike_id"],ride["start_time"])

        db_cur.execute(query,parameters)

        ride_id = db_cur.fetchone()

        db_connection.commit()

        return ride_id[0]


def load_reading_into_database(db_connection : extensions.connection, reading : dict) -> int:
    """Loads an Reading from the reading dict into the database using SQL."""

    with db_connection.cursor() as db_cur:

        query = """INSERT INTO Reading(ride_id,heart_rate,power,rpm,resistance,elapsed_time)
          VALUES (%s,%s,%s,%s,%s,%s) RETURNING reading_id;"""

        parameters = (reading["ride_id"],reading["heart_rate"],reading["power"],
                        reading["rpm"],reading["resistance"],reading["elapsed_time"])

        db_cur.execute(query,parameters)

        reading_id = db_cur.fetchone()

        db_connection.commit()

        return reading_id[0]
    

def load_readings_into_database(db_connection : extensions.connection, readings : list[dict]):
    """Loads a list of readings into the database using SQL."""

    with db_connection.cursor() as db_cur:

        query = """INSERT INTO Reading(resistance, elapsed_time, heart_rate, power, rpm, ride_id)
          VALUES (%s,%s,%s,%s,%s,%s);"""

        parameters = [tuple(reading.values()) for reading in readings]

        db_cur.executemany(query, parameters)

        db_connection.commit()


def select_reading_from_database(db_connection : extensions.connection, reading : dict) -> int:
    """
    Selects a reading id from the database using the reading dict 
    passed in and a SQL Select Query.
    """

    with db_connection.cursor() as db_cur:

        query = """SELECT reading_id FROM Reading WHERE ride_id=%s
            AND elapsed_time=%s"""

        parameters = (reading["ride_id"],reading["elapsed_time"])

        db_cur.execute(query,parameters)

        reading_id = db_cur.fetchone()

        db_connection.commit()

        return reading_id[0]


def load_bike_into_database(db_connection : extensions.connection, bike_serial_number : int) -> int:
    """Loads an Bike into the database using the bikes serial number and SQL."""

    with db_connection.cursor() as db_cur:

        query = """INSERT INTO Bike(serial_number)
          VALUES (%s) RETURNING bike_id;"""

        parameters = (bike_serial_number,)

        db_cur.execute(query,parameters)

        bike_id = db_cur.fetchone()

        db_connection.commit()

        return bike_id[0]


def select_bike_from_database(db_connection : extensions.connection,
                              bike_serial_number : int) -> int:
    """
    Selects a bike id from the database using the bike serial number 
    passed in and a SQL Select Query.
    """

    with db_connection.cursor() as db_cur:

        query = """SELECT bike_id FROM Bike WHERE serial_number=%s"""

        parameters = (bike_serial_number,)

        db_cur.execute(query,parameters)

        bike_id = db_cur.fetchone()

        db_connection.commit()

        return bike_id[0]
