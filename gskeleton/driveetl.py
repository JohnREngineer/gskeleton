import re
import sqlite3
from sqlite3 import Error
from typing import Dict, List, Optional, Union

import gspread
import pandas as pd
import yaml
from oauth2client.service_account import ServiceAccountCredentials
from pydantic import BaseModel
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive


class DriveLocation(BaseModel):
    key: str
    type: Optional[str]
    file_type: Optional[str]


class SheetSpec(BaseModel):
    sheet_id: Union[str, int] = 0
    header_row: int = 0
    start_row: int = 1
    start_column: int = 0


class InputTable(BaseModel):
    name: str
    sheet_spec: SheetSpec = SheetSpec()


class InputDataset(BaseModel):
    name: str
    location: DriveLocation
    tables: List[InputTable]


class SQLCommand(BaseModel):
    text: str


class ETLConfig(BaseModel):
    input_datasets: Optional[List[InputDataset]]
    sql_commands: Optional[List[SQLCommand]]


class DriveETL:
    def __init__(self):
        with open("gskeleton/mime_types.yaml", "r") as stream:
            self.file_types = yaml.safe_load(stream)

    def _get_sorted_keys(
        self, folder_key: str, file_type: str = None
    ) -> List[str]:
        list_file = self.drive.ListFile(
            {"q": "'{}' in parents and trashed=false".format(folder_key)}
        )
        files = list_file.GetList()
        file_infos = []
        for f in files:
            if (not file_type) or (f.get("mimeType") == file_type):
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
        if loc.type == "folder":
            sorted_keys = self._get_sorted_keys(loc.key, loc.file_type)
            key = sorted_keys[0]
        elif loc.type == "file":
            key = loc.key
        else:
            raise ValueError(f"location.type not found for {loc}")
        return key

    def _load_config(self, config_loc: DriveLocation):
        key = self._get_loc_key(config_loc)
        path = self._download_drive_file(key)
        config = None
        with open(path, "r") as stream:
            data = yaml.safe_load(stream)
            config = ETLConfig(**data)
        if config:
            self.config = config
        else:
            raise ValueError(f"Cannot load config file {config_loc}")

    def _get_df_from_workbook(
        self, workbook: gspread.Spreadsheet, spec: SheetSpec
    ):
        sh = None
        if str(spec.sheet_id).isnumeric():
            sh = workbook.get_worksheet(int(spec.sheet_id))
        else:
            sh = workbook.worksheet(spec.sheet_id)
        if not sh:
            val_err = f"Worksheet cannot be found at {spec} in {workbook.id}"
            raise ValueError(val_err)
        df = pd.DataFrame(sh.get_all_values())
        cols = [c for i, c in enumerate(df.columns) if i >= spec.start_column]
        df = df[cols]
        df.columns = df.iloc[spec.header_row]
        df = df.iloc[spec.start_row :]
        df = df.reset_index(drop=True)
        return df

    def _get_sql_col(self, column_name: str):
        lower = column_name.lower()
        words = re.findall(r"\w+", lower)
        col = "_".join(words)
        if not col:
            raise ValueError(f"column name is invalid: {column_name}")
        return col

    def _load_input_tables(self, keys: List[str], tables: List[InputTable]):
        df_lists: Dict[str, List[pd.DataFrame]] = {
            table.name: [] for table in tables
        }
        for key in keys:
            wb = self.gspread_client.open_by_key(key)
            for table in tables:
                df = self._get_df_from_workbook(wb, table.sheet_spec)
                df.columns = [self._get_sql_col(c) for c in df.columns]
                df_lists[table.name].append(df)
        for table in tables:
            df = pd.concat(df_lists[table.name])
            df.to_sql(
                table.name, self._db_conn, if_exists="replace", index=False
            )

    def _extract_input_dataset(self, input_dataset: InputDataset):
        loc = input_dataset.location
        if loc.type == "folder":
            input_keys = self._get_sorted_keys(loc.key, loc.file_type)
            self._load_input_tables(input_keys, input_dataset.tables)
        else:
            raise ValueError(
                f"InputDataset location.type {loc.type} is not valid"
            )

    def load_inputs(self):
        for input_dataset in self.config.input_datasets:
            self._extract_input_dataset(input_dataset)

    def _connect_to_db(self, db_path: str = None):
        try:
            conn_path = db_path or ":memory:"
            self._db_conn = sqlite3.connect(conn_path)
        except Error as e:
            print(e)

    def execute_commands(self):
        cursor = self._db_conn.cursor()
        for command in self.config.sql_commands:
            try:
                cursor.execute(command.text)
                result = cursor.fetchall()
                print(result)
            except Error as e:
                print(e)

    def _close_db(self):
        if self._db_conn:
            self._db_conn.close()
            del self._db_conn

    def run_etl(self, key: str, location_type: Optional[str]):
        config_loc_type = location_type if location_type else "file"
        config_loc_dict = {
            "key": key,
            "type": config_loc_type,
            "file_type": "application/x-yaml",
        }
        config_loc = DriveLocation(**config_loc_dict)
        self._load_config(config_loc)
        self._connect_to_db("DriveETL.db")
        self.load_inputs()
        self.execute_commands()
        self._close_db()
