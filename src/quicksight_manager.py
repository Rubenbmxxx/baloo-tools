import time
import boto3
import uuid
import logging


def refresh_quicksight_spice(spice_dataset_id: str, aws_account_id: str):
    """"
   trigger refresh of a quicksight spive
   :param spice_dataset_id: spice database id
   :param aws_account_id: aws account id
   """
    client = boto3.client('quicksight', region_name='eu-west-1')

    ingestion_id = str(uuid.uuid4())
    response = client.create_ingestion(AwsAccountId=aws_account_id,
                                       DataSetId=spice_dataset_id,
                                       IngestionId=ingestion_id)

    status = None
    while status != 'COMPLETED':
        response = client.describe_ingestion(
            AwsAccountId=aws_account_id,
            DataSetId=spice_dataset_id,
            IngestionId=ingestion_id
        )
        status = response['Ingestion']['IngestionStatus']

        if status in ['FAILED', 'CANCELLED']:
            raise Exception(f"SPICE dataset refresh failed. Status: {status}")

        if status != 'COMPLETED':
            time.sleep(10)  # Wait for 10 seconds before checking status again

    logging.info("SPICE dataset refresh completed successfully!")
