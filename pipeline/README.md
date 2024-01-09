## pipeline.py
Pipeline script to establish connection to Kafka stream, retrieve log lines, and then transform and upload the relevant data to the database (using functions from the `transform.py` and `load.py` files as necessary).

User's max and min heart rates are calculated using functions from `validate_heart_rate.py`, and their heart rate given in the readings compared against them; if it above or below the healthy range too many times in a row (`EXTREME_HR_COUNT_THRESHOLD`), the `validate_heart_rate` function send_email is used to alert the user.

## 🛠️ Getting Setup
- Install requirements using `pip3 install -r requirements.txt`
- Create a `.env` file with the following information:
    - `AWS_ACCESS_KEY_ID_ `= xxxxxxxxxx
    - `AWS_SECRET_ACCESS_KEY_` = xxxxxxxx
    - `DATABASE_USERNAME` = xxxxxxxx
    - `DATABASE_PASSWORD` = xxxxxxxx
    - `DATABASE_IP` = xxxxxxxxx
    - `DATABASE_PORT` = xxxxxxxx
    - `DATABASE_NAME` = xxxxxxxx


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