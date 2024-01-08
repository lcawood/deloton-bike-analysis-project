## Pipeline
The modules in this folder are used to connect to the Kafka stream, fetch and process messages, extract and transform ride, rider and reading information into logically structure atomic parts, and upload to an AWS hosted 3NF database, all the while monitoring live heart-rate readings for riders and alerting them by email about dangerous numbers.

To run the pipeline:
```
# Install requirements for all modules used.
pip install -r requirements.txt
# Start main pipeline script.
python pipeline.py
```

When run, the `pipeline.py` script will run until failure.

## pipeline.py
Pipeline script to establish connection to Kafka stream, retrieve log lines, and then transform and upload the relevant data to the database (using functions from the `transform.py` and `load.py` files as necessary).

User's max and min heart rates are calculated using functions from `validate_heart_rate.py`, and their heart rate given in the readings compared against them; if it above or below the healthy range too many times in a row (`EXTREME_HR_COUNT_THRESHOLD`), the `validate_heart_rate` function send_email is used to alert the user.

### To Run:
`python pipeline.py`

### Requirements:
 - Python modules (as listed in `requirements.txt`):
    - confluent-kafka
    - python-dotenv
 - Environment variables (in `.env` file or otherwise):
    - KAFKA_TOPIC
    - BOOTSTRAP_SERVERS
    - SECURITY_PROTOCOL
    - SASL_MECHANISM
    - USERNAME
    - PASSWORD
    - AWS_ACCESS_KEY_ID_
    - AWS_SECRET_ACCESS_KEY_
    - BUCKET_NAME

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


## load.py

### Requirements:
 - Environment variables (in `.env` file or otherwise):
   - DATABASE_IP
   - DATABASE_USERNAME
   - DATABASE_NAME
   - DATABASE_PASSWORD
   - DATABASE_PORT