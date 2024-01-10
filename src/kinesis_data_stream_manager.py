import boto3
import time
import logging


def put_records(records, stream_name):
    """
    Send json file to Kinesis Data Stream
    :param stream_name: the name of kinesis
    :param records: Records to be sent to Kinesis Stream one by one
    """
    kinesis = boto3.client('kinesis', region_name='eu-west-1')

    batch_size = 100
    max_batch_size = 1024 * 1024  # 1 MB

    if records is None:
        logging.info("the result of the query is empty is empty")
    else:
        for i in range(0, len(records), batch_size):
            batch_records = records[i:i + batch_size]

            put_records_request = {
                'Records': batch_records,
                'StreamName': stream_name
            }

            response = kinesis.put_records(**put_records_request)

            # Check for failed records
            failed_records = response.get('FailedRecordCount')
            if failed_records:
                logging.info(f'{failed_records} records failed to be sent to Kinesis.')

            # Print the response for debugging purposes
            print(response)

            # Check if the payload size exceeds the limit
            payload_size = sum(len(record['Data']) for record in batch_records)
            if payload_size > max_batch_size:
                logging.info(f'Warning: Payload size exceeds the limit of 1 MB. Some records were not sent.')
            time.sleep(0.3)
