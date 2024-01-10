import logging
import os
import sys
import polars as pl

import boto3
import pandas as pd
from pandas import DataFrame
from io import BytesIO

sys.path.append(os.getcwd())  # make sure we had root folder when executing


def save_dataframe_as_parquet(dataframe: DataFrame, bucket_name: str, bucket_key: str):
    try:
        s3_client = boto3.client('s3')
        data_parquet = dataframe.to_parquet()
        s3_client.put_object(Body=data_parquet, Bucket=bucket_name, Key=bucket_key)
    except Exception:
        logging.error(Exception)
        raise


def read_from_s3(bucket_name: str, bucket_key: str):
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket_name, Key=bucket_key)
    information = response['Body'].read().decode('utf-8')

    return information


def save_dataframe_as_csv(dataframe: DataFrame, bucket_name: str, bucket_key: str, sep: str = ',',
                          header: bool = True, quotechar: str = '"'):
    try:
        s3_client = boto3.client('s3')
        data_csv = dataframe.to_csv(index=False, sep=sep, header=header, quotechar=quotechar)
        s3_client.put_object(Body=data_csv, Bucket=bucket_name, Key=bucket_key)
    except Exception:
        logging.error(Exception)
        raise


def save_dataframe_as_json(bucket_name: str, bucket_key: str, df: DataFrame, orient: str, force_ascii: bool = False,
                           lines: bool = True) -> str:
    try:
        s3_client = boto3.client('s3')
        data = df.to_json(orient=orient, force_ascii=force_ascii, lines=lines, date_format='iso', default_handler=str)
        s3_client.put_object(Body=data, Bucket=bucket_name, Key=bucket_key)
    except Exception:
        logging.error(Exception)
        raise


def read_excel(bucket_name: str, bucket_key: str, file_name: str) -> DataFrame:
    if file_name is None:
        raise ValueError(f"the file {file_name} cannot be null")
    else:
        s3_client = boto3.client('s3')
        s3_client.download_file(bucket_name, bucket_key, file_name)
        df = pd.read_excel(file_name)
    return df


def delete_from_s3(bucket_name: str, bucket_key: str):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    objects = bucket.objects.filter(Prefix=bucket_key)
    deleted_objects = objects.delete()


def read_folder_from_s3(bucket_name: str, bucket_key: str):
    s3 = boto3.client('s3')
    contents = []
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=bucket_key)
    for obj in response['Contents']:
        object_key = obj['Key']
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        content = response['Body'].read().decode('utf-8')
        contents.append(content)
    return contents


def write_txt_s3(bucket_name: str, bucket_key: str, text: str):
    try:
        s3_client = boto3.client('s3')
        s3_client.put_object(Body=text, Bucket=bucket_name, Key=bucket_key)
    except Exception:
        logging.error(Exception)
        raise


def read_csv_from_s3(bucket_name: str, bucket_key: str) -> DataFrame:
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=bucket_key)
        data = response['Body'].read()

        df = pl.read_csv(BytesIO(data), has_header=True, truncate_ragged_lines=True)
        return df
    except Exception as e:
        print(f"Error reading CSV from S3: {e}")
        return None  # or raise an exception if appropriate
