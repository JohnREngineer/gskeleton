import sqlite3
from sqlite3 import Error
from typing import Any, List

import gspread
import pandas as pd
import yaml
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive


class DriveETL:
    def __init__(self):
        self.credentials: Any
        # self.gspread_client: gspread.Client
        # self.drive: GoogleDrive
        # self._db_conn: Optional[sqlite3.Connection]

    def _get_keys_from_folder(
        self, key: str, file_type: str = None
    ) -> List[str]:
        mime_types = {
            "yaml": "application/x-yaml",
            "json": "application/json",
            "spreadsheet": "application/vnd.google-apps.spreadsheet",
        }
        list_file = self.drive.ListFile(
            {"q": "'{}' in parents and trashed=false".format(key)}
        )
        files = list_file.GetList()
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

    def _load_yaml(self, path: str) -> object:
        output = None
        with open(path, "r") as stream:
            output = yaml.safe_load(stream)
        return output

    def authorize(self, credentials: Any) -> None:
        self.credentials = credentials
        self.gspread_client = gspread.authorize(self.credentials)
        gauth = GoogleAuth()
        gauth.credentials = self.credentials
        self.drive = GoogleDrive(gauth)

    def _connect_to_db(self, db_path: str = "") -> None:
        try:
            conn_path = db_path or ":memory:"
            conn = sqlite3.connect(conn_path)
            self._db_conn = conn
            print(sqlite3.version)
        except Error as e:
            print(e)

    def _close_db(self) -> None:
        if self._db_conn:
            self._db_conn.close()
            del self._db_conn

    def __get_df_from_drive(
        self, key=None, sheet=0, headers=0, start=1, end=None
    ):
        df, sh = None, None
        if key:
            wb = self.gss_client.open_by_key(self.__sanitize_key(key))
            sh = (
                wb.get_worksheet(int(sheet))
                if str(sheet).isnumeric()
                else wb.worksheet(sheet)
            )
            if not sh:
                raise ValueError("Worksheet cannot be found at %s" % key)
            df = pd.DataFrame(sh.get_all_values())
            df.columns = df.iloc[int(headers)]
            df = df.iloc[int(start) :]  # noqa: E203
            df = df.reset_index(drop=True)
        return df, sh

    def __split_and_replace(
        self, string, split_chars=["\n", "?", "("], replace_chars=[","]
    ):
        out_string = string
        for s in split_chars:
            out_string = out_string.split(s)[0]
        for r in replace_chars:
            out_string = out_string.replace(r, "")
        return out_string

    def _get_dataframe_from_input_keys(self, input_keys, defaults=None):
        dfs = []
        for key in input_keys:
            full_location = defaults.copy()
            full_location.update({"key": key})
            af = self.__get_df_from_drive(**full_location)[0]
            af.columns = [
                self.__split_and_replace(c).strip().upper() for c in af.columns
            ]
            dfs.append(af)
        df = pd.concat(dfs)
        return df

    def run_etl(self, config_folder_key: str) -> None:
        # Import config settings
        config_keys = self._get_keys_from_folder(
            config_folder_key, file_type="yaml"
        )
        self._config_key = config_keys[0]
        self._config_path = self._download_drive_file(self._config_key)
        self._config = self._load_yaml(self._config_path)

        # Initialize DB

        # Load inputs
