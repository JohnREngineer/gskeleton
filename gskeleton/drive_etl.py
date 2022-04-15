import os
import re
import sqlite3
import time
from datetime import datetime as dt
from sqlite3 import Error
from typing import Dict, List, Optional, Union

import gspread
import pandas as pd
import yaml
from oauth2client.service_account import ServiceAccountCredentials
from pydantic import BaseModel
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive


class GFile(BaseModel):
    key: str
    name: Optional[str]


class GFolder(BaseModel):
    key: str
    name: Optional[str]


class GFileSelector(BaseModel):
    folder: GFolder
    top: Union[int, None] = None
    extension: Union[str, None] = None
    order_by: str = "modifiedDate"
    desc = False
    # extension in ["title", "createdDate", "modifiedDate"]
    # order_by in ["json", "gsheet", "xlsx", "yaml", "csv"]


class CellBox(BaseModel):
    header_row: int = 0
    start_row: int = 1
    start_col: int = 0
    end_row: Union[int, None] = None
    end_col: Union[int, None] = None


class Sheet(BaseModel):
    index: int = 0
    name: Optional[str]
    box: CellBox = CellBox()


class Table(BaseModel):
    name: str
    sheet: Sheet = Sheet()


class Extractor(BaseModel):
    name: str
    inputs: GFileSelector
    tables: List[Table]


class Transformer(BaseModel):
    sql_command: str


class Loader(BaseModel):
    name: str
    suffix_type: Optional[str]  # ["unix", "timestamp"]
    extension: str
    template: Optional[GFile]
    exports: GFolder
    tables: List[Table]


class ETLConfig(BaseModel):
    extractors: Optional[List[Extractor]]
    transformers: Optional[List[Transformer]]
    loaders: Optional[List[Loader]]


class DriveETL:
    def __init__(self):
        self.start_unix = str(int(time.time()))
        self.mime_types = {
            "json": "application/json",
            "gsheet": "application/vnd.google-apps.spreadsheet",
            "xlsx": (
                "application/"
                "vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            ),
            "yaml": "application/x-yaml",
            "csv": "text/csv",
        }
        self.config: ETLConfig

    def _select_files(self, fs: GFileSelector) -> List[GFile]:
        def extension_match(file: Dict):
            match = True
            if fs.extension:
                mime_type = self.mime_types.get(fs.extension)
                if mime_type:
                    match = file.get("mimeType") == mime_type
            return match

        query = {"q": f"'{fs.folder.key}' in parents and trashed=false"}
        list_file = self.drive.ListFile(query)
        files = list_file.GetList()
        filtered = filter(extension_match, files)
        sorted_files = sorted(
            filtered, key=(lambda x: x[fs.order_by]), reverse=fs.desc
        )
        return_files = [GFile(key=f["id"]) for f in sorted_files]
        return return_files

    def _download_drive_file(self, file: GFile) -> str:
        f = self.drive.CreateFile({"id": file.key})
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

    def _load_config_from_file(self, config_file: GFile):
        path = self._download_drive_file(config_file)
        config = None
        with open(path, "r") as stream:
            data = yaml.safe_load(stream)
            config = ETLConfig(**data)
        if config:
            self.config = config
        else:
            raise ValueError(f"Cannot load config file {config_file}")

    def _load_config_from_folder(self, folder: GFolder):
        config_selector = GFileSelector(
            folder=folder,
            top=1,
            extension="yaml",
            order_by="modifiedDate",
            desc=True,
        )
        config_files = self._select_files(config_selector)
        self._load_config_from_file(config_files[0])

    def _get_worksheet_box(
        self, worksheet: gspread.Worksheet, box: CellBox
    ) -> pd.DataFrame:
        def between(x: int):
            start = box.start_col <= x
            return (start and (x <= box.end_col)) if box.end_col else start

        df = pd.DataFrame(worksheet.get_all_values())
        cols = [col for ind, col in enumerate(df.columns) if between(ind)]
        df = df[cols]
        df.columns = df.iloc[box.header_row]
        df = df.iloc[box.start_row : box.end_row]  # ignore
        df = df.reset_index(drop=True)
        return df

    def _get_workbook_sheet(
        self, workbook: gspread.Spreadsheet, sheet: Sheet
    ) -> pd.DataFrame:
        worksheet = None
        if sheet.name:
            worksheet = workbook.worksheet(sheet.name)
        else:
            worksheet = workbook.get_worksheet(sheet.index)
        if not worksheet:
            val_err = f"Worksheet cannot be found at {sheet} in {workbook.id}"
            raise ValueError(val_err)
        return self._get_worksheet_box(worksheet, sheet.box)

    def _get_sql_col(self, column_name: str) -> str:
        first_line = column_name.split("\n")[0]
        lower = first_line.lower()
        words = re.findall(r"\w+", lower)
        col = "_".join(words)
        if not col:
            raise ValueError(f"column name is invalid: {column_name}")
        return col

    def _extract_tables(self, extractor: Extractor):
        df_lists: Dict[str, List[pd.DataFrame]] = {
            table.name: [] for table in extractor.tables
        }
        files = self._select_files(extractor.inputs)
        print(files)
        for file in files:
            print(file)
            wb = self.gspread_client.open_by_key(file.key)
            for table in extractor.tables:
                df = self._get_workbook_sheet(wb, table.sheet)
                print(df.columns)
                df.columns = [self._get_sql_col(c) for c in df.columns]
                print(df.columns)
                df_lists[table.name].append(df)
        for table in extractor.tables:
            df = pd.concat(df_lists[table.name])
            df.to_sql(
                table.name, self._db_conn, if_exists="replace", index=False
            )

    def _run_extractors(self):
        for extractor in self.config.extractors:
            self._extract_tables(extractor)

    def _connect_to_db(self, db_path: str = None):
        try:
            conn_path = db_path or ":memory:"
            self._db_conn = sqlite3.connect(conn_path)
        except Error as e:
            print(e)

    def _close_db(self):
        if self._db_conn:
            self._db_conn.close()
            del self._db_conn

    def _run_transformers(self):
        cursor = self._db_conn.cursor()
        for transformer in self.config.transformers:
            try:
                cursor.execute(transformer.sql_command)
                result = cursor.fetchall()
                print(result)
            except Error as e:
                print(e)

    def _get_load_filename(self, loader: Loader) -> str:
        suffix = ""
        if loader.suffix_type == "unix":
            suffix = self.start_unix
        elif loader.suffix_type == "datetime":
            utc = dt.utcfromtimestamp(self.start_unix)
            suffix = utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        elif loader.suffix_type:
            raise ValueError(f"Invalid suffix type: {loader.suffix_type}")
        filename = f"{loader.name}_{suffix}.{loader.extension}"
        return filename

    def _xlsx_load_sheet(self, sheet: Sheet, path: str, df: pd.DataFrame):
        sheet_name = sheet.name
        if not sheet_name:
            xl = pd.ExcelFile(path)
            sheet_name = xl.sheet_names[sheet.index]
        writer_options = {
            "engine": "openpyxl",
            "mode": "a",
            "if_sheet_exists": "replace",
        }
        with pd.ExcelWriter(path, **writer_options) as writer:
            df.to_excel(writer, sheet_name, index=False)

    def _load_tables(self, loader: Loader):
        df_dict: Dict[str, pd.DataFrame] = {}
        for table in loader.tables:
            query = f"SELECT * FROM {table.name};"
            cursor = self._db_conn.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            df = pd.DataFrame(result)
            df_dict[table.name] = df
        if loader.suffix_type == "xlsx":
            load_path = self._get_load_filename(loader)
            if loader.template:
                template_path = self._download_drive_file(loader.template)
                os.rename(template_path, load_path)
            for table in loader.tables:
                self._xlsx_load_sheet(table.sheet, load_path, df)
            create_file_options = {
                "parents": [{"kind": "drive#fileLink", "id": loader.exports}]
            }
            f = self.drive.CreateFile(create_file_options)
            f.SetContentFile(load_path)
            f.Upload()

    def _run_loaders(self):
        for loader in self.config.loaders:
            self._load_tables(loader)

    def run_etl_config(self, key: str, from_folder: bool = False):
        if from_folder:
            self._load_config_from_folder(GFolder(key=key))
        else:
            self._load_config_from_file(GFile(key=key))
        self._connect_to_db("DriveETL.db")
        self._run_extractors()
        self._run_transformers()
        self._run_loaders()
        self._close_db()
