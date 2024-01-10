# BALOO TOOLS

Born on June 21, 2006, Baloo is my feathered child, an Amazona Aestiva. She/he has been close to me since the beginning 
of my data engineering journey.

This repository was intended to be used as internal resource for data engineering purposes at a European Pet Insurance 
company. The name was my colleagues' idea after meeting Baloo. 

Thank you Wiem and Mohamed



## Getting started

Here you will find various Python scripts, where the name of the file refers to what you will find inside.

## aws_secret_manager.py

Python module to read from aws secret manager service 
example:
````
  aws_secrets = get_secret('secret_name')
  key_value = aws_secrets['key_name']
````
## config_manage.py

Python library to make to read a config.ini file 

example of ini file:
````
[ga_web_traffic]
kinesis_data_stream = collection-ga_web_traffic
ga4_properties_id = {
                    'Santevet_BE': '111111111',
                    'Santevet_DE': '222222222',
                    'Santevet_ES': '333333333',
                    'Santevet_FR': '444444444'}

````
example to have the value of ``kinesis_data_stream``

````
config_parser = get_config(bucket_name,key_name)
kinesis_data_stream_value = config_parser.get('ga_web_traffic', 'kinesis_data_stream')
````

## glue_crawler_manager.py

Glue crawler module.

``execute_glue_crawler`` is a function to trigger a scheduler of an existing crawler using his name at the parameter of the function 

## kinesis_data_stream_manager.py

``put_records`` is a function to send records to a kinesis data stream using like a parameter a json record and the kinesis stream name

## mysql_manager.py

Python module for mysql database

``execute_query`` is a function to connect and execute queries at mysql database using like parameters the query, the host name, user's name , the password and the database name  

## quicksight_manager.py

Python module to connect to quicksight 

``refresh_quicksight_spice`` is a function to launch a refresh of a spice in quicksight using spice_dataset_id and the aws_account_id like parameters


## s3_manager

Python module to manage different actions in s3 like :
``save_dataframe_as_parquet`` is a function to save a dataframe in s3 as a parquet file
 
``save_dataframe_as_csv`` is a function to save a dataframe in s3 as a csv file adding optionally the separator, the header and the quotechar
 
``save_dataframe_as_json`` is a function to save a dataframe in s3 as a json file adding the force_ascii the lines like an option
 
``read_from_s3`` is a function to read from s3 
 
``read_excel`` is a function to read from s3 an excel file
 
``delete_from_s3`` is a function to delete an object from s3 bucket

``read_folder_from_s3`` is a function to read  a folder from s3 bucket


## slack_notification.py

Python module to send notification at a slack channel 

example: 
````
default_args = {
    'owner': 'Santevet',
    'depends_on_past': False,
    'start_date': datetime.now(tz=local_tz) - timedelta(days=2),
    'provide_context': True,
    'catchup': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack_fail_alert
}

````

## sql_server_manager

Python module for sql server database

``_sql_server_get_connection`` is a function to connect to sql server database

``execute_query`` is a function to connect and execute queries at sql server database using like parameters the query, the host name, user's name , the password and the database name

## send_mails

Python module to convert a dataframe to a csv file and to send it by mail 
