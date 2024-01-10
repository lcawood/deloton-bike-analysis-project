"""Module to contain and run the endpoints for the Deloton staff API"""

from datetime import datetime
from flask import Flask, request,current_app, abort, Response
from flask_caching import Cache

import api_functions
from database_functions import get_database_connection


app = Flask(__name__)
app.json.sort_keys = False
cache = Cache(config={'CACHE_TYPE': 'SimpleCache', 'CACHE_DEFAULT_TIMEOUT': 4})
cache.init_app(app)
db_conn = get_database_connection()


def reset_database_connection():
    """Function to reset database connection."""
    global db_conn
    db_conn.close()
    db_conn = get_database_connection()


def is_not_get_request(*args, **kwargs) -> bool:
    """
    Function to return False is request method is GET, and TRUE otherwise;
    used to ensure only GET requests are cached.
    """
    if request.method == "GET":
        return False
    return True


@app.route("/", methods=["GET"])
def index():
    """ Creates an index route with an index page for the API """
    return current_app.send_static_file('./pages/main_index.html')


@app.route("/ride", methods=["GET"])
def default_ride_endpoint():
    """Default ride endpoint"""
    return current_app.send_static_file('./pages/default_ride_index.html')
        

@app.route("/ride/<int:ride_id>", methods=["GET", "DELETE"])
@cache.cached(query_string=True, unless = is_not_get_request)
def ride_endpoint(ride_id: int):
    """GET or DELETE ride with specified id."""
    match request.method:
        case "GET":
            expanded = request.args.get('expanded', 'False').title()
            summary = request.args.get('summary', 'False').title()
            response = api_functions.get_ride(db_conn, ride_id, expanded, summary)
        case "DELETE":
            response = api_functions.delete_ride(db_conn, ride_id)
        
    # Resets db_conn if server error
    if response[0] == api_functions.STATUS_CODES['server error']:
        reset_database_connection()

    return response


@app.route("/rider", methods=["GET"])
def default_rider_endpoint():
    """Default rider endpoint"""
    return current_app.send_static_file('./pages/default_rider_index.html')


@app.route("/rider/<int:rider_id>", methods=["GET"])
@cache.cached(query_string=True)
def rider_endpoint(rider_id: int):
    """Endpoint to return a rider information JSON by id."""
    response = api_functions.get_rider(db_conn, rider_id)

    # Resets db_conn if server error
    if response[0] == api_functions.STATUS_CODES['server error']:
        reset_database_connection()

    return response


@app.route("/rider/<int:rider_id>/rides", methods=["GET"])
@cache.cached(query_string=True)
def rider_rides_endpoint(rider_id: int):
    """Endpoint to return a JSON of all rides belonging to a rider of specified id."""
    expanded = request.args.get('expanded', 'False').title()
    summary = request.args.get('summary', 'False').title()
    response = api_functions.get_rider_rides(db_conn, rider_id, expanded, summary)

    # Resets db_conn if server error
    if response[0] == api_functions.STATUS_CODES['server error']:
        reset_database_connection()
    return response


@app.route("/daily", methods=["GET"])
@cache.cached(query_string=True)
def daily_rides_endpoint():
    """Get all of the rides in the specified day - defaults to current day."""
    date = request.args.get('date', datetime.today().strftime("%d-%m-%Y"))
    expanded = request.args.get('expanded', 'False').title()
    summary = request.args.get('summary', 'False').title()
    response = api_functions.get_daily_rides(db_conn, date, expanded, summary)

    # Resets db_conn if server error
    if response[0] == api_functions.STATUS_CODES['server error']:
        reset_database_connection()
    return response


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
