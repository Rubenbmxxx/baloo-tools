import pysftp


def _get_connection(ssh_private_key_path: str, host: str, username: str):
    try:
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        # Connecting to SFTP Server
        sftp = pysftp.Connection(host=host,
                                 username=username, cnopts=cnopts, private_key=ssh_private_key_path)
        return sftp
    except Exception as e:
        raise Exception(e, "SFTP Connection Error")



def send_to_sftp(sftp_local_directory, sftp_filename, ssh_private_key_path: str, host_sftp: str, user_name_sftp: str):
    """

    :param sftp_local_directory: the path of the file we want to send
    :param sftp_filename: the name of the file to send
    :param ssh_private_key_path: the path to the ssh_private_key file
    :param host_sftp : the host of sftp
    :param user_name_sftp
    :return:
    """
    sftp = None

    try:
        # Create a connection to the SFTP server
        sftp = _get_connection(ssh_private_key_path, host_sftp, user_name_sftp)

        # Upload the CSV file to the SFTP server
        sftp.put(sftp_local_directory, sftp_filename)
        print(f"Dataframe sent as CSV via SFTP: {sftp_filename}")
    except Exception as e:
        raise
    finally:
        if sftp is not None:
            sftp.close()
