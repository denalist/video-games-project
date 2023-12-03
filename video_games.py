import argparse
import logging
import os
from io import StringIO

import boto3
import pandas as pd
from botocore.exceptions import (ClientError, NoCredentialsError,
                                 PartialCredentialsError)

# config logger
logging.basicConfig(
    filename="app.log",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.DEBUG,
)
logger = logging.getLogger(__name__)


def preprocess_df(df: pd.DataFrame, session, bucket_name, destination):
    # Group rows by year
    # add a colum for year to speed up search
    try:
        df["release_year"] = df["release_date"].dt.year

        for year in df["release_year"].unique():
            rows = df[df["release_year"] == year]
            write_bucket_file(session, bucket_name, year, rows, destination)
    except Exception as e:
        logger.exception(f" Faled to preprocess the data frame to s3")


# def parse_sys_inputs():
#     # TOOD: Add description
#     parser = argparse.ArgumentParser()

#     # TODO: Use shortcuts, e.g. -s for source path, -v fore verbosity etc
#     # TODO: Use separate function for parser
#     parser.add_argument("source_path", type=str, help="Data source csv file required.")
#     parser.add_argument("destination", type=str, help="destination of output files")
#     parser.add_argument("--verbosity", help="increase verbosity")
#     args = parser.parse_args()

#     # TODO: print or log.debug?
#     if args.verbosity:
#         print("verbosity turned on")

#     return args


def read_aws_key():
    # read env
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    if not aws_access_key_id or not aws_secret_access_key:
        raise ValueError(" AWS credentials are not set in environment!")
    else:
        return aws_access_key_id, aws_secret_access_key


def connect_s3(key_id, secret):
    try:
        s3_client = boto3.client(
            "s3", aws_access_key_id=key_id, aws_secret_access_key=secret
        )
        logger.info("successfully connected to s3")
        return s3_client
    except (NoCredentialsError, PartialCredentialsError, ClientError) as e:
        logger.exception(f"Failed to connect to s3 {e}")


def read_bucket_file(session, bucket_name, file_name):
    try:
        s3 = session.resource("s3")
        obj = s3.Object(bucket_name, file_name)
        # convert bytes to string
        return StringIO(str(obj.get()["Body"].read(), "utf-8"))
    except ClientError as e:
        logger.exception(f" Failed to read file from s3 {e}")


def write_bucket_file(
    session, bucket_name, release_year, rows: pd.DataFrame, destination
):
    file_path = f"{destination}year={release_year}/"
    logger.info(f" ---- File path to be put in {file_path} -----")

    try:
        csv_buffer = StringIO()
        rows.to_csv(csv_buffer)

        s3 = session.resource("s3")
        object = s3.Object(bucket_name, f"{file_path}records.csv")
        object.put(Body=csv_buffer.getvalue())
        logger.info(
            f"success in saving year {release_year} with number of records {len(rows)}"
        )
    except ClientError as e:
        logger.exception(f" Client Error when put file: {e}")
    except Exception as e:
        logger.exception(f"An exception has occurred when put file: {e}")


def main():
    key_id, secret = read_aws_key()
    # s3_client = connect_s3(key_id, secret)
    session = boto3.Session(key_id, secret)
    bucket_file = read_bucket_file(
        session, "videogames-jason", "landing/video_games.csv"
    )
    data = pd.read_csv(bucket_file, parse_dates=["release_date"])
    preprocess_df(data, session, "videogames-jason", "treated/")


if __name__ == "__main__":
    main()

# TODO: Step 1: put code into lambda
#       - read_aws_key() may not be necessary, how to check if running as lambda vs. local?
#       - write files to S3 still

# TODO: Step 2: parameterise via JSON body -- pass parameters via JSON to specify bucket name, bucket prefix, etc.anything necessary

# TODO: Step 3: trigger lambda on file upload (to /landing) to S3 using eventbridge (tricky)