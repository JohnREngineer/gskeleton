from typing import Any, List

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

    def _get_keys_from_folder(
        self, key: str, file_type: str = None
    ) -> List[str]:
        mime_types = {
            "yaml": "application/x-yaml",
            "json": "application/json",
            "spreadsheet": "application/vnd.google-apps.spreadsheet",
        }
        files = self.drive.ListFile(
            {"q": "'{}' in parents and trashed=false".format(key)}
        ).GetList()
        type_select = mime_types[file_type] if file_type else ""
        output_files = [
            (f.get("modifiedDate"), f.get("id"))
            for f in files
            if (file_type is None) or (f.get("mimeType") == type_select)
        ]
        sorted_files = sorted(output_files, key=(lambda x: x[0]), reverse=True)
        sorted_keys = [f[1] for f in sorted_files]
        return sorted_keys

    def _download_drive_file(self, key: str):
        f = self.drive.CreateFile({"id": key})
        f.FetchMetadata(fetch_all=True)
        path = f.metadata["title"]
        f.GetContentFile(path)
        return path

    def run_etl(self, config_folder_key: str):
        # Download config settings
        config_keys = self._get_keys_from_folder(config_folder_key)
        config_path = self._download_drive_file(config_keys[0])
        return config_path
