"""
A script then when ran creates a html string a document that show
a daily report of the rides from the previous day.
The html body is returned in a lambda handler.
The html document is sent to a S3 Bucket.
"""

from os import environ
import json

from dotenv import load_dotenv

def handler(event=None, context=None) -> int:
    """Handler for the lambda function"""

    try:

        load_dotenv()

        report_dict = create_report_data([])

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