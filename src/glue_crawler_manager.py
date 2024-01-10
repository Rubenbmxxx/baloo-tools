from airflow.contrib.hooks.aws_hook import AwsHook
import time


def execute_glue_crawler(crawler_name: str):
    """"
   trigger a glue crawler
   :param crawler_name: glue crawler name
   """
    # Create a Boto3 session using the AWSHook
    aws_hook = AwsHook(aws_conn_id='aws_default')
    session = aws_hook.get_session()

    # Create a Glue client
    glue_client = session.client(f'glue', region_name='eu-west-1')  # Replace with your desired region

    response = glue_client.start_crawler(
        Name=crawler_name
    )

    state = None
    # Wait until the current schedule completes
    while state != 'READY':
        response = glue_client.get_crawler(
            Name=crawler_name
        )
        state = response['Crawler']['State']
        status = response['Crawler']['LastCrawl']['Status']
        if status not in ['Completed', 'COMPLETED']:
            time.sleep(10)

        if response == ['FAILED']:
            raise Exception(f"the crawler {crawler_name} is: {status}")
