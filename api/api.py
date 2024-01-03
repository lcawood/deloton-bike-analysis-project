"""Module to contain and run the endpoints for the Deloton staff API"""

from flask import Flask, request
from flask_caching import Cache
import json

import api_functions
from database_functions import get_database_connection


app = Flask(__name__)
app.json.sort_keys = False
cache = Cache(config={'CACHE_TYPE': 'SimpleCache', 'CACHE_DEFAULT_TIMEOUT': 1})
cache.init_app(app)
db_conn = get_database_connection()


def is_not_get_request(*args, **kwargs) -> bool:
    """
    Function to return False is request method is GET, and TRUE otherwise;
    used to ensure only GET requests are cached.
    """
    if request.method == "GET":
        return False
    return True


@app.route("/ride/<int:ride_id>", methods=["GET", "DELETE"])
@cache.cached(query_string=True, unless = is_not_get_request)
def ride_endpoint(ride_id: int):
    """GET or DELETE ride with specified id."""
    match request.method:
        case "GET":
            return api_functions.get_ride(db_conn, ride_id)
        case "DELETE":
            return api_functions.delete_ride(db_conn, ride_id)


@app.route("/rider/<int:rider_id>", methods=["GET"])
@cache.cached(query_string=True)
def rider_endpoint(rider_id: int):
    """Endpoint to return a rider information JSON by id."""
    return api_functions.get_rider(db_conn, rider_id)


@app.route("/rider/<int:rider_id>/rides", methods=["GET"])
@cache.cached(query_string=True)
def rider_rides_endpoint(rider_id: int):
    """Endpoint to return a JSON of all rides belonging to a rider of specified id."""
    expanded = request.args.get('expanded')
    summary = request.args.get('summary')
    return api_functions.get_rider_rides(db_conn, rider_id, expanded, summary)


@app.route("/daily", methods=["GET"])
@cache.cached(query_string=True)
def daily_rides_endpoint():
    """Get all of the rides in the specified day - defaults to current day."""
    date = request.args.get('date')
    if date is None:
        return api_functions.get_daily_rides(db_conn)

    return api_functions.get_daily_rides(db_conn, date)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
