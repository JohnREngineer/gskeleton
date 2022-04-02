import pandas as pd

from gskeleton.driveetl import DriveETL


class MockedListFile:
    def __init__(self, get_list):
        self.GetList = lambda: get_list


class MockedCreateFile:
    def __init__(self):
        self.GetContentFile = lambda x: None

    def FetchMetadata(self, fetch_all=None):
        self.metadata = {"title": "mocked title"}


def test_drive_etl_init(mocker):
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

    keys = etl._get_folder_keys("test_config_folder_key")
    assert keys == ["test_key3", "test_key2", "test_key1"]

    keys = etl._get_folder_keys("test_config_folder_key", "yaml")
    assert keys == ["test_key2", "test_key1"]

    keys = etl._get_folder_keys("test_config_folder_key", "json")
    assert keys == ["test_key3"]

    create_file = MockedCreateFile()
    mocker.patch(
        "gskeleton.driveetl.GoogleDrive.CreateFile", return_value=create_file
    )
    path = etl._download_drive_file("file_to_download")
    assert path == "mocked title"

    mocked_config_yaml = mocker.mock_open(read_data="mocked config settings")
    mocker.patch("builtins.open", mocked_config_yaml)
    yaml = etl._load_yaml("test config")
    assert yaml == "mocked config settings"


def test_sqlite_init(mocker):
    etl = DriveETL()
    etl._connect_to_db(":memory:")
    assert etl._db_connection is not None
    data = {
        "product_name": ["Computer", "Tablet", "Monitor", "Printer"],
        "price": [900, 300, 450, 150],
    }
    df = pd.DataFrame(data, columns=["product_name", "price"])
    df.to_sql("products", etl._db_connection, if_exists="replace", index=False)

    tables_query = """SELECT name FROM sqlite_master
                      WHERE type='table';"""
    cursor = etl._db_connection.cursor()
    cursor.execute(tables_query)
    result = cursor.fetchall()
    assert result == [("products",)]

    products_query = """SELECT * FROM products;"""
    cursor = etl._db_connection.cursor()
    cursor.execute(products_query)
    result = cursor.fetchall()
    assert result == [
        ("Computer", 900),
        ("Tablet", 300),
        ("Monitor", 450),
        ("Printer", 150),
    ]

    etl._close_db()
