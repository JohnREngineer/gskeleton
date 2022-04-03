import sqlite3
from sqlite3 import Error
from typing import Any, Dict, List, Optional, Union

import gspread
import pandas as pd
import yaml
from oauth2client.service_account import ServiceAccountCredentials
from pydantic import BaseModel
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive


class DriveLocation(BaseModel):
    key: str
    location_type: Optional[str]
    file_type: Optional[str]


class SheetSpec(BaseModel):
    sheet_id: Union[str, int] = 0
    header_row: int = 0
    start_row: int = 1


class InputTable(BaseModel):
    name: str
    sheet_spec: SheetSpec = SheetSpec()


class InputDataset(BaseModel):
    name: str
    location: DriveLocation
    tables: List[InputTable]


class ETLConfig(BaseModel):
    input_datasets: List[InputDataset]


class DriveETL:
    def _get_sorted_keys(
        self, folder_key: str, file_type: str = None
    ) -> List[str]:
        mime_types = {
            "yaml": "application/x-yaml",
            "json": "application/json",
            "spreadsheet": "application/vnd.google-apps.spreadsheet",
        }
        list_file = self.drive.ListFile(
            {"q": "'{}' in parents and trashed=false".format(folder_key)}
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

    def service_auth(self, secret_path: str) -> None:
        self.gspread_client = gspread.service_account(filename=secret_path)
        scope = [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/spreadsheets",
        ]
        gauth = GoogleAuth()
        gauth.auth_method = "service"
        gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(
            secret_path, scope
        )
        self.drive = GoogleDrive(gauth)

    def _get_loc_key(self, loc: DriveLocation) -> str:
        key = ""
        if loc.location_type == "folder":
            sorted_keys = self._get_sorted_keys(loc.key, loc.file_type)
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

    def _get_df_from_workbook(
        self, workbook: gspread.Spreadsheet, spec: SheetSpec
    ):
        sh = None
        if str(spec.sheet_id).isnumeric():
            sh = workbook.get_worksheet(spec.sheet_id)
        else:
            sh = workbook.worksheet(spec.sheet_id)
        if not sh:
            val_err = f"Worksheet cannot be found at {spec} in {workbook.id}"
            raise ValueError(val_err)
        df = pd.DataFrame(sh.get_all_values())
        df.columns = df.iloc[spec.header_row]
        df = df.iloc[spec.start_row :]
        df = df.reset_index(drop=True)
        return df

    def _simple_column_name(self, string):
        split_chars = ["\n", "?", "("]
        replace_chars = [","]
        out_string = string.strip().upper()
        for s in split_chars:
            out_string = out_string.split(s)[0]
        for r in replace_chars:
            out_string = out_string.replace(r, "")
        return out_string

    def _load_input_tables(self, keys: List[str], tables: List[InputTable]):
        df_lists: Dict[str, List[pd.DataFrame]] = {
            table.name: [] for table in tables
        }
        for key in keys:
            wb = self.gspread_client.open_by_key(key)
            for table in tables:
                df = self._get_df_from_workbook(wb, table.sheet_spec)
                df.columns = [self._simple_column_name(c) for c in df.columns]
                df_lists[table.name].append(df)
        for table in tables:
            df = pd.concat(df_lists[table.name])

    def _extract_input_dataset(self, input_dataset: InputDataset):
        loc = input_dataset.location
        if loc.location_type == "folder":
            input_keys = self._get_sorted_keys(loc.key, loc.file_type)
            self._load_input_tables(input_keys, input_dataset.tables)
        else:
            raise ValueError(
                f"InputDataset location_type {loc.location_type} is not valid"
            )

    def load_inputs(self):
        for input_dataset in self.config.input_datasets:
            self._extract_input_dataset(input_dataset)

    def _connect_to_db(self, db_path: str = None) -> None:
        try:
            conn_path = db_path or ":memory:"
            conn = sqlite3.connect(conn_path)
            self._db_conn = conn
        except Error as e:
            print(e)

    def _close_db(self) -> None:
        if self._db_conn:
            self._db_conn.close()
            del self._db_conn

    def run_etl(self, key: str, location_type: Optional[str]) -> None:
        config_loc_type = location_type if location_type else "file"
        config_vars = {
            "key": key,
            "location_type": config_loc_type,
            "file_type": "yaml",
        }
        config_loc = DriveLocation(**config_vars)
        # Import config settings
        self.config = self._get_config(config_loc)

        # Initialize DB
        self._connect_to_db()

        # Load inputs
        self.load_inputs()
