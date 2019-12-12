from datetime import datetime

import boto3
import pandas as pd
from pyathena import connect

class AthenaQuery(object):
    """A AthenaQuery for manipulation of s3 data through Athena service

    Attributes:
        aws_region_name: An string representing the aws region-name
        athena_bucket_path: An string representing the athena bucket path
    """

    def __init__(self):
        """Return a new AthenaQuery object."""
        self.aws_region_name = "ap-southeast-1"
        self.athena_bucket_path = "s3://aws-athena-query-results-358002497134-ap-southeast-1/"

    def connect(self):
        self.athena_conn, self.athena_cursor = self.connect_to_athena()
        self.boto_s3_client = self.connect_to_s3()

    def connect_to_athena(self):
        """Return athena_conn athena_cursor object"""
        athena_conn = connect(s3_staging_dir=self.athena_bucket_path, region_name=self.aws_region_name)
        athena_cursor = athena_conn.cursor()
        return athena_conn, athena_cursor

    def connect_to_s3(self):
        """Return s3_client  object"""
        session = boto3.session.Session()
        s3_client = boto3.client('s3')
        return s3_client
    
    def query(self, query, print_debug_messages = False):
        """
        Assume that this doesn't respect data types / returns everything as object
        
        Returns a pandas dataframe
        """
        return self.query_athena_via_s3_to_dataframe(query, self.athena_cursor, self.boto_s3_client)
    
    def query_athena_via_s3_to_dataframe(self, query, athena_cursor, boto_s3_client, print_debug_messages=False):
        """
        This is a lot faster than using the cursors etc of classic python db querying.

        Note that this doesn't know the data types TODO: possibly add ability to pass dictionary to the function
        Example for the boto_s3_client:
        session = boto3.session.Session()
        boto_s3_client = boto3.client('s3')
        """
        if print_debug_messages:
            print("Running query")
        ap_start_time = datetime.now()

        athena_cursor.execute(query)
        ap_query_done_time = datetime.now()
        query_duration_seconds = (ap_query_done_time - ap_start_time).total_seconds()
        if print_debug_messages:
            print("Query finished after %.2f" % query_duration_seconds)
            print("Now going to get the results from S3")

        s3_start_time = datetime.now()
        bucket, key = athena_cursor.output_location.strip('s3://').split('/', 1)
        response = boto_s3_client.get_object(Bucket=bucket, Key=key)
        file = response["Body"]

        df = pd.read_csv(file)
        s3_end_time = datetime.now()
        s3_read_seconds = (s3_end_time - s3_start_time).total_seconds()
        if print_debug_messages:
            print("Reading the CSV from S3 (not running query) took %.2f" % s3_read_seconds)

        ap_done_time = datetime.now()
        ap_total_seconds = (ap_done_time - ap_start_time).total_seconds()
        if print_debug_messages:
            print("All finished using as_pandas in %.2f" % ap_total_seconds)
        return df
