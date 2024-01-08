"""Module to contain and run the endpoints for the Deloton staff API"""

from datetime import datetime
from flask import Flask, request,current_app
from flask_caching import Cache

import api_functions
from database_functions import get_database_connection

DEFAULT_RIDE_HTML = """
Enter a ride number to see its details:
<ul>
    <li> Follow this with <i>?</i> and: </li>
    <ul>
        <li> <i>expanded=True</i> to see all the readings for the ride; </li>
        <li> <i>summary=True</i> to see a summary of the readings for the ride; </li>
        <li> <i>expanded=True&summary=True</i> to see all the readings and a summary for the ride. </li>
    </ul>
</ul>
<br>
Example:
<pre><i>    /ride/1?expanded=True&summary=True</i></pre>
"""
DEFAULT_RIDER_HTML = """
Enter a rider number to see their details:
<ul>
    <li> Follow this with <i>/rides</i> to see all of their logged rides: </li>
    <ul>
        <li> Follow this with <i>?</i> and: </li>
        <ul>
            <li> <i>expanded=True</i> to see all the readings from each ride; </li>
            <li> <i>summary=True</i> to see a summary of the readings from each ride; </li>
            <li> <i>expanded=True&summary=True</i> to see all the readings, with a summary, from each ride. </li>
        </ul>
    </ul>
</ul>
<br>
Example:
<pre><i>    /rider/1/rides?expanded=True&summary=True</i></pre>
"""


app = Flask(__name__)
app.json.sort_keys = False
cache = Cache(config={'CACHE_TYPE': 'SimpleCache', 'CACHE_DEFAULT_TIMEOUT': 1})
cache.init_app(app)


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
    return current_app.send_static_file('./web_pages/index.html')

@app.route("/ride", methods=["GET"])
@cache.cached(query_string=True)
def default_ride_endpoint():
    """Default ride endpoint"""
    return DEFAULT_RIDE_HTML, api_functions.STATUS_CODES['success']
        

@app.route("/ride/<int:ride_id>", methods=["GET", "DELETE"])
@cache.cached(query_string=True, unless = is_not_get_request)
def ride_endpoint(ride_id: int):
    """GET or DELETE ride with specified id."""
    match request.method:
        case "GET":
            expanded = request.args.get('expanded', 'False').title()
            summary = request.args.get('summary', 'False').title()
            return api_functions.get_ride(db_conn, ride_id, expanded, summary)
        case "DELETE":
            return api_functions.delete_ride(db_conn, ride_id)


@app.route("/rider", methods=["GET"])
@cache.cached(query_string=True)
def default_rider_endpoint():
    """Default rider endpoint"""
    return DEFAULT_RIDER_HTML, api_functions.STATUS_CODES['success']


@app.route("/rider/<int:rider_id>", methods=["GET"])
@cache.cached(query_string=True)
def rider_endpoint(rider_id: int):
    """Endpoint to return a rider information JSON by id."""
    return api_functions.get_rider(db_conn, rider_id)


@app.route("/rider/<int:rider_id>/rides", methods=["GET"])
@cache.cached(query_string=True)
def rider_rides_endpoint(rider_id: int):
    """Endpoint to return a JSON of all rides belonging to a rider of specified id."""
    expanded = request.args.get('expanded', 'False').title()
    summary = request.args.get('summary', 'False').title()
    return api_functions.get_rider_rides(db_conn, rider_id, expanded, summary)


@app.route("/daily", methods=["GET"])
@cache.cached(query_string=True)
def daily_rides_endpoint():
    """Get all of the rides in the specified day - defaults to current day."""
    date = request.args.get('date', datetime.today().strftime("%d-%m-%Y"))
    expanded = request.args.get('expanded', 'False').title()
    summary = request.args.get('summary', 'False').title()

    return api_functions.get_daily_rides(db_conn, date, expanded, summary)


if __name__ == "__main__":
    db_conn = get_database_connection()
    app.run(debug=True, host="0.0.0.0", port=5000)
