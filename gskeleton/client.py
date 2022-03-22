from pydantic import BaseModel


class DriveLocation(BaseModel):
    key: str


class Client:
    def __init__(self, settings_key: str):
        self.settings_location = DriveLocation(key=settings_key)