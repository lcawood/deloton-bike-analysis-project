## pipeline.py
Python script to establish connection to Kafka stream, retrieve log lines, and then transform and upload the relevant data to the database (using functions from the `transform.py` and `load.py` files as necessary).

### To Run:
`python pipeline.py`

### Requirements:
 - Python modules (as listed in `requirements.txt`):
    - confluent-kafka
    - python-dotenv
 - Enviroment variables (in `.env` file or otherwise):
    - KAFKA_TOPIC=deloton
    - BOOTSTRAP_SERVERS=pkc-l6wr6.europe-west2.gcp.confluent.cloud:9092r
    - SECURITY_PROTOCOL=SASL_SSL
    - SASL_MECHANISM=PLAIN
    - USERNAME=RK2BFUEPXKGZN3VL
    - PASSWORD=XFkrzeHVBwPcsFaSPEaFvc00zdvJKGS8e3v/Uxpb/YvzJmW/tG9lNMuofhiZD43D

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