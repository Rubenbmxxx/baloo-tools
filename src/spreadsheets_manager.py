import json
import gspread
from googleapiclient.discovery import build
from google.oauth2 import service_account
from oauth2client.service_account import ServiceAccountCredentials


def download_google_sheet(credentials_path: str, file_id: str, sheet_number: int = 0):
    """
    Retrieve non-formatted data from a Google Sheet at Google Drive.
    You must convert the output for example as DataFrame pandas.DataFrame()

    :param credentials_path: Google Service Account credential to access the file.
    :param file_id: Google Sheet file id. e.x. 1JPxhR_AoNhBAK5rd0ECF9Gh_gUB_T7qoBF3SaVwm20Q
    :param sheet_number: Sheet number we want to retrieve.
    :return: Non formatted values.
    """

    # Authenticate with Google Sheets using credentials
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(credentials_path), scope)
    client = gspread.authorize(creds)

    # Open the Google Sheet by ID
    sheet = client.open_by_key(file_id)

    # Get the first sheet of the workbook (index 0)
    worksheet = sheet.get_worksheet(sheet_number)

    # Read the data from the sheet
    data = worksheet.get_all_values()

    return data


def download_excel_file(credentials_path: str, file_id: str):
    """
    Retrieve non-formatted data from a XLSX file at Google Drive.
    You must convert the output for example as DataFrame:
    pandas.read_excel(response,header=None,skiprows=5,names=excel_headers,sheet_name=sheet_name)

    :param credentials_path: Google Service Account credential to access the file.
    :param file_id: XLSX file id. e.x. 1DI8Fc9HTuK-gPGRKovFcV-fDkJxKvh4R
    :return: Non formatted values.
    """

    # Google Drive Service Account logging
    credentials = service_account.Credentials. \
        from_service_account_info(json.loads(credentials_path),
                                  scopes=['https://www.googleapis.com/auth/drive.readonly'])

    # Connect to the Google Drive API
    drive_service = build('drive', 'v3', credentials=credentials)

    # Download the file as a BytesIO object
    request = drive_service.files().get_media(fileId=file_id)
    response = request.execute()

    return response
