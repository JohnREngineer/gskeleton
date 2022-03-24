from typing import Any

import gspread
from pydantic import BaseModel
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive


class DriveLocation(BaseModel):
    key: str


class DriveETL:
    def __init__(self, credentials: Any):
        self.credentials = credentials
        self.gc = gspread.authorize(self.credentials)
        gauth = GoogleAuth()
        gauth.credentials = self.credentials
        self.drive = GoogleDrive(gauth)

    def run_etl(self, settings_key: str):
        self.settings_location = DriveLocation(key=settings_key)
