import gspread
from oauth2client.client import GoogleCredentials
from pydantic import BaseModel
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive


class DriveLocation(BaseModel):
    key: str


class DriveETL:
    def __init__(self, settings_key: str):
        self._set_credentials_from_application_default()
        self.settings_location = DriveLocation(key=settings_key)

    def _set_credentials_from_application_default(self) -> None:
        self.default_credentials = GoogleCredentials.get_application_default()
        self.spread = gspread.authorize(self.default_credentials)
        gauth = GoogleAuth()
        gauth.credentials = self.default_credentials
        self.drive = GoogleDrive(gauth)
