"""
gskeleton
~~~~~~~~~

Google Drive ETL library.

"""
__version__ = "0.1.0"
__author__ = "John R"


from oauth2client.client import GoogleCredentials

from .driveetl import DriveETL, DriveLocation


def authorize(credentials: GoogleCredentials) -> DriveETL:
    etl = DriveETL()
    etl.authorize(credentials)
    return etl
