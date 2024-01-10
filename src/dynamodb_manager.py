import boto3
from boto3.dynamodb.conditions import Key


def get_dynamodb_resource():
    dynamodb_resource = boto3.resource('dynamodb', region_name='eu-west-1')
    return dynamodb_resource


def read_dynamodb_resource(table_name: str, filter_column: str, filter_value: str):
    # Create a DynamoDB resource
    dynamodb_resource = get_dynamodb_resource()

    # Get the DynamoDB table
    table = dynamodb_resource.Table(table_name)

    response = table.query(
        KeyConditionExpression=Key(filter_column).eq(filter_value)
    )

    return response['Items']


def update_dynamodb_resource(table_name: str, filter_column: str, filter_value: str, update_column: str,
                             update_value: str):
    # Create a DynamoDB resource
    dynamodb_resource = get_dynamodb_resource()

    # Get the DynamoDB table
    table = dynamodb_resource.Table(table_name)

    # Update the defined column
    table.update_item(
        Key={
            filter_column: filter_value
        },
        UpdateExpression=f"set {update_column} = :r",
        ExpressionAttributeValues={
            ':r': update_value,
        },
        ReturnValues="UPDATED_NEW"
    )


def check_value_exists(table_name: str, column_name: str, column_value: str):
    dynamodb = get_dynamodb_resource()
    table = dynamodb.Table(table_name)
    response = table.query(
        KeyConditionExpression=Key(column_name).eq(column_value)
    )

    return response


def insert_dynamodb_resource(table_name: str, items_to_insert):
    """
        :param table_name: the name of the table
        :param items_to_insert: the column with them values with a json format
        for example:

        items_to_insert={
            'product_name': product_name,
            'last_update': last_update_date
         }
     insert_dynamodb_resource("test_table",items_to_insert)

    """
    dynamodb = get_dynamodb_resource()
    table = dynamodb.Table(table_name)
    table.put_item(Item=items_to_insert)


def delete_dynamodb_resource(table_name: str, key):
    """

    :param table_name: the name of dynamoDB table
    :param key: the raw to delete with json format
    :exemple :
    key= {
        'product_name': product_name,
        'last_update': item_to_delete
    }

    delete_dynamodb_resource("test_table",key)

    """
    dynamodb = get_dynamodb_resource()
    table = dynamodb.Table(table_name)
    table.delete_item(Key=key)
