# Daily Report

This folder contains all the code and resources used for creating the daily report.

# 📝 Project Description

 - A script was created to transfer a previous days worth of data from the Deloton RDS to a Pandas Dataframe. This dataframe was used to extract key statistics from the ride data that could be used in a daily report to the Deloton ceo.

- This script ensures that it fetches only the previous days data. The data fetched is placed into a html string where its uploaded as a html report to a S3 Bucket on AWS. The html string is also returned in the body of a lambda handler so it can be accessed on AWS Lambda to send an email using SES V2.

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


## 🏃 Running the daily report locally

Run the command `python3 report.py`

## 🗂️ Files Explained
- `report.py`
    - A script to extract the previous days data from the Deloton data warehouse then using this create useful data deliverables to present to the ceo in a report. The report is made in a html string then uploaded to a S3 Bucket as a html file. The html string is also the return from the lambda handler function as this can be used in a email to the CEO.
- `test_report.py`
    - A script containing unit tests for the `report.py` script
- `Dockerfile`
    - A file which is used to build a Docker image of the lambda for the `report.py` program
    - To build this image, run the command `docker build -t <image-name> .`
    - To run the container, run the command `docker run --env-file .env <image-name>`
 
## ☁️ Cloud Architecture:

![Daily Report Architecture Diagram](../diagrams/Deloton_Daily_Report_Architecture_Diagram.png)
