import configparser
from baloo_tools.s3_manager import read_from_s3


def _get_config_file(filepath: str):
    with open(filepath) as f:
        config_file = f.read()
    return config_file


def get_config_from_file(filepath: str):
    """
    Get a config file resources from a file.
    This file must be referenced from the project folder

    :param filepath: Config File location. (e.g. dags/santevet_data_vetolib/config/vetolib_data_sources.ini)
    """
    config_file = _get_config_file(filepath)
    config_parser = configparser.ConfigParser()
    config_parser.read_string(config_file)
    return config_parser


def get_value_from_config(config_file, config_section, config_key):
    """
    Get a config file value from a file.
    This file must be referenced from the project folder

    :param config_file: Config File location. (e.g. dags/santevet_data_vetolib/config/vetolib_data_sources.ini)
    :param config_section: Section from config file. (e.g. [contacts])
    :param config_key: Key value from config section (e.g. api_endpoint_product)
    """
    config_parser = get_config_from_file(config_file)
    config_key_value = config_parser.get(config_section, config_key)
    return config_key_value


def get_config(s3_bucket_name: str, s3_prefix: str):
    """
    Get a config file resources from a file at S3

    :param s3_bucket_name: (e.g. mwaa-data-dev-20230330090515821400000002 )
    :param s3_prefix: (e.g. dags/santevet_data_vetolib/config/vetolib_data_sources.ini)
    """
    config_file = read_from_s3(s3_bucket_name, s3_prefix)
    config_parser = configparser.ConfigParser()
    config_parser.read_string(config_file)
    return config_parser
