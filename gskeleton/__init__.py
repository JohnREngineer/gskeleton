"""
gskeleton
~~~~~~~~~

Google Drive ETL library.

"""
__version__ = "0.1.0"
__author__ = "John R"


from typing import Any

from .driveetl import DriveETL


def authorize(credentials: Any) -> DriveETL:
    etl = DriveETL(credentials)
    return etl
