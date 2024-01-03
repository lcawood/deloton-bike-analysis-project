"""
A script then when ran creates a html string a document that show
a daily report of the rides from the previous day.
The html body is returned in a lambda handler.
The html document is sent to a S3 Bucket.
"""

from os import environ, _Environ, remove
import json
from datetime import datetime,timedelta

from boto3 import client
from dotenv import load_dotenv
import psycopg2
from psycopg2 import extensions
import pandas as pd

def get_s3_client(config: _Environ):
    """Get a connection to the relevant S3 bucket."""
    s3_client = client("s3",
                       aws_access_key_id=config["AWS_ACCESS_KEY_ID"],
                       aws_secret_access_key=config["AWS_SECRET_ACCESS_KEY"])
    return s3_client

def get_database_connection() -> extensions.connection:
    """Return a connection our database."""

    return psycopg2.connect(user=environ["DATABASE_USERNAME"],
                            password=environ["DATABASE_PASSWORD"],
                            host=environ["DATABASE_IP"],
                            port=environ["DATABASE_PORT"],
                            database=environ["DATABASE_NAME"]
                            )

def create_html_string(ride_dict : dict,yesterday : datetime) -> str:
    """Creates a html string to create tables of data for the email report"""

    current_values = 0

    html_style = """
<head>
<style>
table {
}

td, th {
border: 1px solid #dddddd;
text-align: left;
padding: 8px;
}

tr:nth-child(even) {
background-color: #dddddd;
}
</style>
</head>"""
    body_beginning = f"""<body>

<h2>Daily Report for {yesterday}</h2>
<p>&nbsp;&nbsp;&nbsp;&nbsp;</p>
<h3>Collected Ride Data</h3>
<table>
<tr>
    <th>Metric</th>
    <th>Value</th>
</tr>
<tr>"""

    values_in_html = f"""<td>Number of rides completed in the past day</td>
<td>{current_values}</td>
</tr>
<tr>
<td>Gender split of riders of the past day</td>
<td>{current_values}</td>
</tr>
<tr>
<td>Ages of the riders of the past day</td>
<td>{current_values}</td>
</tr>
<tr>
<td>Average power and heart rate of riders of past day</td>
<td>{current_values}</td>
</tr>
</table>
</body>"""
    updated_html = html_style + body_beginning + values_in_html

    updated_html = updated_html.replace("\n", "")

    return updated_html

def sql_select_all_useful_data(db_connection : extensions.connection) -> pd.DataFrame:
    """Uses SQL to select all the needed data to create the useful report for the ceo"""
    query = """SELECT Ride.ride_id,Ride.rider_id,Ride.bike_id,Ride.start_time,
    Rider.gender, AVG(Reading.heart_rate) FROM Ride INNER JOIN
    Rider ON Rider.rider_id = Ride.rider_id INNER JOIN
    Reading ON Reading.ride_id = Ride.ride_id
    WHERE DATE(start_time) = '2023-10-05'
    GROUP BY Ride.ride_id,Rider.gender;"""

    rides = pd.read_sql_query(query,db_connection)

    print(rides.groupby('rider_id', as_index=False)['avg'].mean())

    return rides

def create_report_data(rides : pd.DataFrame, yesterday :datetime, db_connection : extensions.connection) -> dict:
    """Creates and calls all the data needed in the report"""

    report_data_dict = {}

    data_dict = sql_select_all_useful_data(db_connection)

    print(data_dict)

    exported_html = create_html_string(report_data_dict, yesterday)

    return {"html_body": exported_html}

def create_html_file(html_string : str, yesterday : datetime) -> None:
    """Creates a HTML document based on the html string for the report"""

    # Creating the HTML file
    file_html = open(f"daily_report_{yesterday}.html", "w")

    # Adding the input data to the HTML file
    file_html.write(html_string)

    # Saving the data into the HTML file
    file_html.close()


def upload_file(s3_client,filename: str , bucket : str, key : str) -> None:
    """Uploads a file to a s3 bucket"""

    s3_client.upload_file(filename,bucket,key)

def remove_old_file(yesterday):
    """Removes the old data from the directory"""
    try:
        remove(f"././daily_report_{yesterday}.html")
    except FileNotFoundError:
        pass

def handler(event=None, context=None) -> int:
    """Handler for the lambda function"""

    try:

        yesterday = datetime.now().date() #- timedelta(days=1)

        load_dotenv()
        connection = get_database_connection()
        s3_client = get_s3_client(environ)

        report_dict = create_report_data({},yesterday,connection)
        create_html_file(report_dict["html_body"],yesterday)

        upload_file(s3_client,f"./daily_report_{yesterday}.html", "c9-deloton-daily-reports",
            f"c9-deloton-daily-reports/daily_report_{yesterday}.html")
        
        remove_old_file(yesterday)

        return {
                'statusCode': 200,
                'body': json.dumps(report_dict["html_body"])
        }   
    
    except Exception as e:
        return {
            'statusCode': 200,
            'body': json.dumps(e)
        }



if __name__ == "__main__":
    print(handler())