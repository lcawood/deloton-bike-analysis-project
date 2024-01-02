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
      pass

      

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

            return rider_id
