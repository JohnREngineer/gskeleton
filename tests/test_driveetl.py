import pandas as pd
import pytest

from gskeleton.driveetl import DriveETL


class MockedListFile:
    def __init__(self, get_list):
        self.GetList = lambda: get_list


class MockedCreateFile:
    def __init__(self):
        self.GetContentFile = lambda x: None

    def FetchMetadata(self, fetch_all=None):
        self.metadata = {"title": "mocked title"}


def test_col_names(mocker):
    etl = DriveETL()
    with pytest.raises(ValueError):
        etl._get_sql_col("")
    with pytest.raises(ValueError):
        etl._get_sql_col(" ")
    with pytest.raises(ValueError):
        etl._get_sql_col("   ")
    with pytest.raises(ValueError):
        etl._get_sql_col("\t")
    with pytest.raises(ValueError):
        etl._get_sql_col("\t\t\t")
    with pytest.raises(ValueError):
        etl._get_sql_col("\t \t")
    with pytest.raises(ValueError):
        etl._get_sql_col("!")
    with pytest.raises(ValueError):
        etl._get_sql_col("!@")
    with pytest.raises(ValueError):
        etl._get_sql_col("! @")
    with pytest.raises(ValueError):
        etl._get_sql_col("!\t @")
    assert etl._get_sql_col("A") == "a"
    assert etl._get_sql_col("A1") == "a1"
    assert etl._get_sql_col("A_1") == "a_1"
    assert etl._get_sql_col("A1b2C3") == "a1b2c3"
    assert etl._get_sql_col("123") == "123"
    assert etl._get_sql_col("a b") == "a_b"
    assert etl._get_sql_col(" a b ") == "a_b"
    assert etl._get_sql_col("a   b") == "a_b"
    assert etl._get_sql_col("\ta\tb\t") == "a_b"
    assert etl._get_sql_col("a\t\t\tb") == "a_b"
    assert etl._get_sql_col("a!b") == "a_b"
    assert etl._get_sql_col("!a!b!") == "a_b"
    assert etl._get_sql_col("! a ! b !") == "a_b"
    assert etl._get_sql_col("! \tA1! !b2@!\t!C3#\t ! \tD4 !") == "a1_b2_c3_d4"


def test_drive_etl_auth(mocker):
    mock_gspread_auth = mocker.patch(
        "gskeleton.driveetl.gspread.service_account",
        return_value="mocked_gspread_client",
    )
    mock_drive_auth = mocker.patch(
        "gskeleton.driveetl.ServiceAccountCredentials.from_json_keyfile_name",
        return_value="mocked_drive_credentials",
    )
    mock_google_drive = mocker.patch(
        "gskeleton.driveetl.GoogleDrive", return_value="mocked_drive"
    )

    etl = DriveETL()
    scope = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    service_path = "/test/test_location.json"
    etl.service_auth(service_path)
    assert mock_gspread_auth.call_args[1]["filename"] == service_path
    assert etl.gspread_client == "mocked_gspread_client"
    assert etl.drive == "mocked_drive"
    assert mock_drive_auth.call_args[0][0] == service_path
    assert mock_drive_auth.call_args[0][1] == scope
    gauth = mock_google_drive.call_args[0][0]
    assert gauth.auth_method == "service"
    assert gauth.credentials == "mocked_drive_credentials"


def test_drive_etl_init(mocker):
    mocker.patch(
        "gskeleton.driveetl.gspread.service_account",
        return_value="mocked_gspread_client",
    )
    mocker.patch(
        "gskeleton.driveetl.ServiceAccountCredentials.from_json_keyfile_name",
        return_value="mocked_drive_credentials",
    )

    etl = DriveETL()
    assert etl.file_types.get("yaml") is not None
    etl.service_auth("test_path.json")
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

    keys = etl._get_sorted_keys("test_config_folder_key")
    assert keys == ["test_key3", "test_key2", "test_key1"]

    keys = etl._get_sorted_keys("test_config_folder_key", "application/x-yaml")
    assert keys == ["test_key2", "test_key1"]

    keys = etl._get_sorted_keys("test_config_folder_key", "application/json")
    assert keys == ["test_key3"]

    mocker.patch(
        "gskeleton.driveetl.GoogleDrive.CreateFile",
        return_value=MockedCreateFile(),
    )
    path = etl._download_drive_file("file_to_download")
    assert path == "mocked title"

    mocked_config_yaml = mocker.mock_open(read_data="mocked config settings")
    mocker.patch("builtins.open", mocked_config_yaml)

    # TODO: test _load_config


def test_sqlite_init(mocker):
    etl = DriveETL()
    etl._connect_to_db(":memory:")
    assert etl._db_conn is not None
    data = {
        "product_name": ["Computer", "Tablet", "Monitor", "Printer"],
        "price": [900, 300, 450, 150],
    }
    df = pd.DataFrame(data, columns=["product_name", "price"])
    df.to_sql("products", etl._db_conn, if_exists="replace", index=False)

    tables_query = """SELECT name FROM sqlite_master
                      WHERE type='table';"""
    cursor = etl._db_conn.cursor()
    cursor.execute(tables_query)
    result = cursor.fetchall()
    assert result == [("products",)]

    products_query = """SELECT * FROM products;"""
    cursor = etl._db_conn.cursor()
    cursor.execute(products_query)
    result = cursor.fetchall()
    assert result == [
        ("Computer", 900),
        ("Tablet", 300),
        ("Monitor", 450),
        ("Printer", 150),
    ]

    etl._close_db()
