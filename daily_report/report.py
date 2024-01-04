"""
A script then when ran creates a html string a document that show
a daily report of the rides from the previous day.
The html body is returned in a lambda handler.
The html document is sent to a S3 Bucket.
"""

# pylint: disable=C0301
# pylint: disable=W0613

from os import environ, _Environ
import json
from datetime import datetime,timedelta,date

from boto3 import client
from dotenv import load_dotenv
import psycopg2
from psycopg2 import extensions
import pandas as pd

def get_s3_client(config: _Environ):
    """Get a connection to the relevant S3 bucket."""
    s3_client = client("s3",
                       aws_access_key_id=config["AWS_ACCESS"],
                       aws_secret_access_key=config["AWS_SECRET_ACCESS"])
    return s3_client

def get_database_connection() -> extensions.connection:
    """Return a connection our database."""

    return psycopg2.connect(user=environ["DATABASE_USERNAME"],
                            password=environ["DATABASE_PASSWORD"],
                            host=environ["DATABASE_IP"],
                            port=environ["DATABASE_PORT"],
                            database=environ["DATABASE_NAME"]
                            )

def create_gender_split_table(gender_dict : dict) -> str:
    """Creates html string for multiple lines in a table based on the gender dict size"""

    if gender_dict == []:
        gender_dict = [{'gender': 'female', 'count': 0}, {'gender': 'male', 'count': 0}]

    html_string = ""

    for gender in gender_dict:
        html_string += f"""
<tr>
<td>
{gender["gender"]}
</td>
<td>
{gender["count"]}
</td>
</tr>
"""

    html_string += "</table>"
    return html_string

def create_user_stats_table(age_dict : dict,power_dict : dict, heart_dict : dict) -> str:
    """Creates html string for multiple lines in a table based on the gender dict size"""

    html_string = ""

    for i in range(len(age_dict)):
        html_string += f"""
<tr>
<td>
{age_dict[i]["rider_id"]}
</td>
<td>
{age_dict[i]["Age"]}
</td>
<td>
{power_dict[i]["average_power"]}
</td>
<td>
{heart_dict[i]["avg"]}
</td>
</tr>
"""

    html_string += "</table>"
    return html_string


def create_html_string(ride_dict : dict,yesterday : datetime) -> str:
    """Creates a html string to create tables of data for the email report"""

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

    table_insert_one = f"""<td>Number of rides completed in the past day</td>
<td>{ride_dict["amount_of_rides"]}</td>
</tr>
</table>
<p>&nbsp;&nbsp;&nbsp;&nbsp;</p>
<h3>Gender split of riders</h3>
<table>
<tr>
    <th>Gender</th>
    <th>Count</th>
</tr>
"""
    gender_split = create_gender_split_table(ride_dict["gender"])

    table_insert_two = """
<p>&nbsp;&nbsp;&nbsp;&nbsp;</p>
<h3>Statistics from Riders</h3>
<table>
<tr>
    <th>Rider ID</th>
    <th>Age</th>
    <th>Average Heart Rate</th>
    <th>Average Power</th>
</tr>
"""

    user_stats = create_user_stats_table(ride_dict["ages"],ride_dict["power"],
                                         ride_dict["heart_rate"])

    final = "</body>"

    updated_html = html_style + body_beginning + table_insert_one + gender_split + table_insert_two + user_stats + final

    updated_html = updated_html.replace("\n", "")

    return updated_html

def previous_day_from_database(db_connection : extensions.connection) -> str:
    """Gets the previous day based on the most recent entry to the database"""

    query = """SELECT start_time
    FROM Ride 
    ORDER BY start_time DESC 
    LIMIT 1"""

    with db_connection.cursor() as db_cur:
        db_cur.execute(query)

        row = db_cur.fetchone()

        db_connection.commit()

    last_time = row[0]
    last_date = last_time.date() - timedelta(days=1)
    date_time = last_date.strftime("%Y-%m-%d")
    return date_time

def sql_select_all_useful_data(db_connection : extensions.connection) -> pd.DataFrame:
    """Uses SQL to select all the needed data to create the useful report for the ceo"""

    calculate_previous_day = previous_day_from_database(db_connection)


    query = f"""SELECT Ride.ride_id,Ride.rider_id,Ride.bike_id,Ride.start_time,
    Rider.gender,Rider.birthdate,AVG(Reading.heart_rate),AVG(Reading.power) as average_power FROM Ride INNER JOIN
    Rider ON Rider.rider_id = Ride.rider_id INNER JOIN
    Reading ON Reading.ride_id = Ride.ride_id
    WHERE DATE(start_time) = '{calculate_previous_day}'
    GROUP BY Ride.ride_id,Rider.gender,Rider.birthdate;"""

    rides = pd.read_sql_query(query,db_connection)

    return rides

def convert_to_age(born):
    """Converts a date of birth to the age of the person"""

    born = datetime.strptime(born, "%Y-%m-%d").date()
    today = date.today()
    return today.year - born.year - ((today.month,
                                      today.day) < (born.month,
                                                    born.day))

def extract_report_data(rides : pd.DataFrame) -> dict:
    """Extracts all the data needed to populate the report"""

    rides['birthdate']=rides['birthdate'].astype(str)
    rides['Age'] = rides['birthdate'].apply(convert_to_age)

    number_of_rides = rides.ride_id.count()

    average_power_users = rides.groupby('rider_id', as_index=False)['average_power'].mean()
    average_heart_rate_users = rides.groupby('rider_id', as_index=False)['avg'].mean()

    grouped_users = rides.groupby('rider_id').agg({'Age': 'mean', 'gender': lambda x: x.mode().iat[0]}).reset_index()

    gender_split = grouped_users.groupby(['gender'], as_index=False)['rider_id'].count()
    gender_split = gender_split.rename(columns={"rider_id": "count"})

    riders_age_df = rides.groupby('rider_id')['Age'].mean().reset_index()
    riders_age_df['Age'] = riders_age_df['Age'].astype(int)

    power = average_power_users.to_dict('records')
    heart_rate = average_heart_rate_users.to_dict('records')
    gender = gender_split.to_dict('records')
    ages = riders_age_df.to_dict('records')

    return {"power" : power,"heart_rate" : heart_rate,"amount_of_rides": number_of_rides,
            "gender": gender, "ages" : ages}

def create_report_data(yesterday :datetime, db_connection : extensions.connection) -> dict:
    """Creates and calls all the data needed in the report"""

    rides_df = sql_select_all_useful_data(db_connection)

    report_data_dict = extract_report_data(rides_df)

    exported_html = create_html_string(report_data_dict, yesterday)

    return {"html_body": exported_html}


def handler(event=None, context=None) -> int:
    """Handler for the lambda function"""

    try:

        load_dotenv()
        connection = get_database_connection()
        s3_client = get_s3_client(environ)

        yesterday = previous_day_from_database(connection)

        report_dict = create_report_data(yesterday,connection)

        s3_client.put_object(Body = report_dict["html_body"],Bucket = "c9-deloton",
            Key = f"c9-deloton-daily-reports/daily_report_{yesterday}.html")

        connection.close()

        return {
                'statusCode': 200,
                'body': json.dumps(report_dict["html_body"])
        }

    except Exception as e:
        return {
            'statusCode': 404,
            'body': json.dumps(str(e))
        }



if __name__ == "__main__":
    print(handler())
