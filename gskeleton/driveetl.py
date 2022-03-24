from typing import Any

import gspread
from pydantic import BaseModel
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive


class DriveLocation(BaseModel):
    key: str


class DriveETL:
    def __init__(self, settings_key: str, credentials: Any):
        self.settings_location = DriveLocation(key=settings_key)
        self.credentials = credentials
        self.gc = gspread.authorize(self.credentials)
        gauth = GoogleAuth()
        gauth.credentials = self.credentials
        self.drive = GoogleDrive(gauth)
