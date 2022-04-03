"""
gskeleton
~~~~~~~~~

Google Drive ETL library.

"""
__version__ = "0.1.0"
__author__ = "John R"

from .driveetl import DriveETL


def authorize(secret_path: str) -> DriveETL:
    etl = DriveETL()
    etl.service_auth(secret_path)
    return etl
