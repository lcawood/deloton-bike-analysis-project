FROM python:latest

WORKDIR /usr/src/app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY validate_heart_rate.py .
COPY transform.py .
COPY load.py .
COPY database_functions.py .
COPY pipeline.py .


CMD python3 pipeline.py