from gskeleton.driveetl import DriveETL


class MockedListFile:
    def __init__(self, get_list):
        self.GetList = lambda: get_list


class MockedCreateFile:
    def __init__(self):
        self.GetContentFile = lambda x: None

    def FetchMetadata(self, fetch_all=None):
        self.metadata = {"title": "mocked title"}


def test_get_keys_from_folder(mocker):
    mocker.patch(
        "gskeleton.driveetl.gspread.authorize",
        return_value="test_gspread_credentials",
    )
    etl = DriveETL()
    etl.authorize("test_credentials")
    my_list = [
        {
            "id": "test_key1",
            "mimeType": "application/x-yaml",
            "modifiedDate": "1",
        },
        {
            "id": "test_key2",
            "mimeType": "application/x-yaml",
            "modifiedDate": "2",
        },
        {
            "id": "test_key3",
            "mimeType": "application/json",
            "modifiedDate": "3",
        },
    ]
    list_file = MockedListFile(my_list)
    mocker.patch(
        "gskeleton.driveetl.GoogleDrive.ListFile", return_value=list_file
    )

    keys = etl._get_keys_from_folder("test_config_folder_key")
    assert keys == ["test_key3", "test_key2", "test_key1"]

    keys = etl._get_keys_from_folder("test_config_folder_key", "yaml")
    assert keys == ["test_key2", "test_key1"]

    keys = etl._get_keys_from_folder("test_config_folder_key", "json")
    assert keys == ["test_key3"]

    create_file = MockedCreateFile()
    mocker.patch(
        "gskeleton.driveetl.GoogleDrive.CreateFile", return_value=create_file
    )
    path = etl._download_drive_file("file_to_download")
    assert path == "mocked title"

    etl.run_etl("test_config_folder_key")
    assert etl.config_key == "test_key2"
