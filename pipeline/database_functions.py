"""Module containing functions used to interact with the RDS database."""

from os import environ

from dotenv import load_dotenv
import psycopg2
from psycopg2 import extensions

load_dotenv()

def get_database_connection() -> extensions.connection:
    """Return a connection our database"""

    return psycopg2.connect(user=environ["DATABASE_USERNAME"],
                            password=environ["DATABASE_PASSWORD"],
                            host=environ["DATABASE_IP"],
                            port=environ["DATABASE_PORT"],
                            database=environ["DATABASE_NAME"]
                            )


def load_address_into_database(db_connection : extensions.connection, address : dict) -> int:
    """Loads an Address into the database using SQL"""

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
      """Selects a address id from the database using the address dict passed in and a SQL Select Query"""
      
      with db_connection.cursor() as db_cur:
            
            query = """SELECT address_id FROM Address WHERE first_line=%s 
            AND postcode=%s"""

            parameters = (address["first_line"],address["postcode"])

            db_cur.execute(query,parameters)

            address_id = db_cur.fetchone()

            db_connection.commit()

            return address_id[0]

      

def load_user_into_database(db_connection : extensions.connection, user : dict) -> int:
    """Loads a user into the database using SQL."""

    with db_connection.cursor() as db_cur:
       
            query = """INSERT INTO Rider(rider_id,address_id,first_name,last_name,birthdate,height,weight,
            email,gender,account_created) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING rider_id;"""

            parameters = (user['user_id'],user['address_id'],user['first_name'],
                          user['last_name'],user['birthdate'],user['height'],
                          user['weight'],user['email'],user['gender'],user['account_created'])

            db_cur.execute(query,parameters)

            rider_id = db_cur.fetchone()

            return rider_id[0]

def load_ride_into_database(db_connection : extensions.connection, ride : dict) -> int:
    """Loads an Ride into the database using SQL"""

    with db_connection.cursor() as db_cur:
       
            query = """INSERT INTO Ride(rider_id,bike_id,start_time) 
            VALUES (%s,%s,%s) RETURNING ride_id;"""

            parameters = (ride["user_id"],ride["bike_id"],ride["start_time"])

            db_cur.execute(query,parameters)

            ride_id = db_cur.fetchone()

            db_connection.commit()

            return ride_id[0]

def select_ride_from_database(db_connection : extensions.connection, ride : dict) -> int:
      """Selects a ride id from the database using the ride dict passed in and a SQL Select Query"""
      
      with db_connection.cursor() as db_cur:
            
            query = """SELECT ride_id FROM Ride WHERE user_id=%s 
            AND bike_id=%s AND start_time =%s"""

            parameters = (ride["user_id"],ride["bike_id"],ride["start_time"])

            db_cur.execute(query,parameters)

            ride_id = db_cur.fetchone()

            db_connection.commit()

            return ride_id[0]