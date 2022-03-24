from typing import Any

import gspread
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive


class DriveETL:
    def __init__(self, credentials: Any):
        self.credentials = credentials
        self.gc = gspread.authorize(self.credentials)
        gauth = GoogleAuth()
        gauth.credentials = self.credentials
        self.drive = GoogleDrive(gauth)

    def _get_from_folder(self, key: str):
        files = self.drive.ListFile(
            {"q": "'{}' in parents and trashed=false".format(key)}
        ).GetList()
        selected = [
            {"key": f.get("id"), "mimeType": f.get("mimeType")} for f in files
        ]
        return selected

    def run_etl(self, config_key: str):
        # Download config settings
        self.config_key = config_key
        return self._get_from_folder(self.config_key)
