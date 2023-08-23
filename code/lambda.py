# Importing the libraries
import json
import pandas as pd
import urllib3
import boto3
import os
import logging
import psycopg2

# For tracking events
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Defining the keys to store raw and cleaned data in S3 bucket
s3 = boto3.client("s3")
s3_key_raw = "Data/gamesGiveaway.json"
s3_key_clean = "Data/giveawayCleanedd.csv"

# URL for API
http = urllib3.PoolManager()
url = "https://www.gamerpower.com/api/giveaways"

# Lambda function
def lambda_handler(event, context):
    try:
        response = http.request('GET', url)
        if response.status == 200:
            data = json.loads(response.data.decode('utf-8'))  # Parse JSON data
            s3_bucket_raw = "apprentice-training-data-kushal-raw"
            s3.put_object(Bucket=s3_bucket_raw, Key=s3_key_raw, Body=json.dumps(data))  # Putting raw data into raw bucket

            # Transform data to DataFrame and drop columns
            df = pd.DataFrame(data)
            transformed_data = df.drop(columns=['thumbnail', 'image', 'published_date'])  # TR 1: dropping irrelevant columns
            transformed_data.rename(columns=lambda x: x.upper(), inplace=True)  # TR 2: Changing the column names to UPPERCASE
            transformed_data = transformed_data.replace(to_replace="N/A", value="0")  # TR 3: Changing the default N/A value to new default value 0

            transformed_csv = transformed_data.to_csv(index=False)
            s3_bucket_clean = "apprentice-training-data-kushal-cleaned"
            s3.put_object(Bucket=s3_bucket_clean, Key=s3_key_clean, Body=transformed_csv)  # Putting cleaned data to clean bucket

            logger.info(msg="Dumped cleaned data")

            conn = psycopg2.connect(
                host=os.environ['DB_HOST'],
                database=os.environ['DB_NAME'],
                user=os.environ['DB_USER'],
                password=os.environ['DB_PASSWORD']
            )

            cur = conn.cursor()

            for index, row in transformed_data.iterrows():
                cur.execute(
                    "INSERT INTO apprentice_kushal (ID, TITLE, WORTH, DESCRIPTION, INSTRUCTIONS, OPEN_GIVEAWAY_URL, TYPE, PLATFORMS, END_DATE, USERS, STATUS, GAMERPOWER_URL, OPEN_GIVEAWAY) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        row['ID'],
                        row['TITLE'],
                        row['WORTH'],
                        row['DESCRIPTION'],
                        row['INSTRUCTIONS'],
                        row['OPEN_GIVEAWAY_URL'],
                        row['TYPE'],
                        row['PLATFORMS'],
                        row['END_DATE'],
                        row['USERS'],
                        row['STATUS'],
                        row['GAMERPOWER_URL'],
                        row['OPEN_GIVEAWAY']
                    )
                )

            conn.commit()

    except Exception as e:
        logger.error(msg="Error: " + str(e))
        if conn:
            conn.rollback()

    finally:
        if conn:
            conn.close()

    return {
        'statusCode': 200,
        'body': 'Data processed and uploaded to S3 and RDS'
    }
