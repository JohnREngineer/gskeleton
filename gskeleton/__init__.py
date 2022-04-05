"""
gskeleton
~~~~~~~~~

Google Drive ETL library.

"""
__version__ = "0.1.0"
__author__ = "John R"

from .drive_etl import DriveETL


def authorize(secret_path: str) -> DriveETL:
    etl = DriveETL()
    etl.service_auth(secret_path)
    return etl
