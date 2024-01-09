## pipeline.py
Pipeline script to establish connection to Kafka stream, retrieve log lines, and then transform and upload the relevant data to the database (using functions from the `transform.py` and `load.py` files as necessary).

User's max and min heart rates are calculated using functions from `validate_heart_rate.py`, and their heart rate given in the readings compared against them; if it above or below the healthy range too many times in a row (`EXTREME_HR_COUNT_THRESHOLD`), the `validate_heart_rate` function send_email is used to alert the user.

## 🛠️ Getting Setup
- Install requirements using `pip3 install -r requirements.txt`

## 🔐 Environment Variables
- Create a `.env` file with the following information:
- `DATABASE_IP` -> ARN to your AWS RDS.
- `DATABASE_NAME` -> Name of your database.
- `DATABASE_USERNAME` -> Your database username.
- `DATABASE_PASSWORD` -> Password to access your database.
- `DATABASE_PORT` -> Port used to access the database.
- `AWS_ACCESS_KEY_ID_ `  -> Personal AWS ACCESS KEY available on AWS.
- `AWS_SECRET_ACCESS_KEY_` -> Personal AWS SECRET ACCESS KEY available on AWS.
- `KAFKA_TOPIC` -> The current kafka topic to subscribe to.
- `BOOTSTRAP_SERVERS` -> Name of the kafka server.
- `SECURITY_PROTOCOL` -> Name of the security protocol for the kafka stream.
- `SASL_MECHANISM` -> Name of the simple username/password authentication mechanism for the kafka stream.
- `USERNAME` -> Username for kafka account to access the input stream.
- `PASSWORD` -> Password for kafka account to access the input stream.
- `BUCKET_NAME` -> Name of the S3 Bucket all data gets sent to.

## 🏃 Running the pipeline locally

Run the command `python3 pipeline.py`

### Dependencies:
 - `transform.py`
    - `get_user_from_log_line`
    - `get_ride_data_from_log_line`
    - `get_reading_data_from_log_line`
 - `load.py`
    - `add_user`
    - `add_ride`
    - `add_reading`
    - `add_bike`
 - `validate_heart_rate`
    - `calculate_max_heart_rate`
    - `calculate_min_heart_rate`