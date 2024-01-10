import json
import boto3
import logging


def get_secret(secret_name: str):
    """"
    Retrieve AWS Secret Manager SecretString as dict
    :param secret_name: Secret key name
    """
    try:
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name='eu-west-1')
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return json.loads(secret)

        return json.loads(get_secret_value_response["SecretString"])
    except Exception:
        logging.error(Exception)
        raise


def get_secret_key_value(secret_name: str, key_name: str):
    secret = get_secret(secret_name)
    value = secret.get(key_name, None)
    return value
