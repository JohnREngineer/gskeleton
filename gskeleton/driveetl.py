import sqlite3
from sqlite3 import Error
from typing import Any, List, Optional, Union

import gspread
import pandas as pd
import yaml
from pydantic import BaseModel
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive


class DriveLocation(BaseModel):
    location_type: str
    key: str
    file_type: Optional[str]


class SheetParams(BaseModel):
    sheet: Union[str, int]
    header_row: int
    start_row: int


class InputTable(BaseModel):
    name: str
    sheet_params: SheetParams = SheetParams(sheet=0, header_row=0, start_row=1)


class InputDataset(BaseModel):
    location: DriveLocation
    tables: List[InputTable]


class ETLConfig(BaseModel):
    input_datasets: List[InputDataset]


class DriveETL:
    def __init__(self):
        self.credentials: Any
        # self.gspread_client: gspread.Client
        # self.drive: GoogleDrive
        # self._db_conn: Optional[sqlite3.Connection]

    def _get_folder_keys(self, key: str, file_type: str = None) -> List[str]:
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
        file_infos = []
        for f in files:
            if (file_type is None) or (f.get("mimeType") == type_select):
                file_info = (f.get("modifiedDate"), f.get("id"))
                file_infos.append(file_info)
        sorted_files = sorted(file_infos, key=(lambda x: x[0]), reverse=True)
        sorted_keys = [f[1] for f in sorted_files]
        return sorted_keys

    def _download_drive_file(self, key: str):
        f = self.drive.CreateFile({"id": key})
        f.FetchMetadata(fetch_all=True)
        path = f.metadata["title"]
        f.GetContentFile(path)
        return path

    def _load_yaml(self, path: str) -> Any:
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

    def _get_loc_key(self, loc: DriveLocation) -> Any:
        key = ""
        if loc.location_type == "folder":
            sorted_keys = self._get_folder_keys(loc.key, loc.file_type)
            key = sorted_keys[0]
        elif loc.location_type == "file":
            key = loc.key
        else:
            raise ValueError(f"location_type not found for {loc}")
        return key

    def _get_config(self, config_loc: DriveLocation) -> ETLConfig:
        key = self._get_loc_key(config_loc)
        path = self._download_drive_file(key)
        data = self._load_yaml(path)
        config = ETLConfig(**data)
        return config

    def run_etl(self, config_loc: DriveLocation) -> None:
        # Import config settings
        self.config = self._get_config(config_loc)

        # Initialize DB

        # Load inputs
