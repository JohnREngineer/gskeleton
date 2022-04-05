import pandas as pd
import pytest

from gskeleton.drive_etl import DriveETL, GFile, GFileSelector, GFolder


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
        "gskeleton.drive_etl.gspread.service_account",
        return_value="mocked_gspread_client",
    )
    mock_drive_auth = mocker.patch(
        "gskeleton.drive_etl.ServiceAccountCredentials.from_json_keyfile_name",
        return_value="mocked_drive_credentials",
    )
    mock_google_drive = mocker.patch(
        "gskeleton.drive_etl.GoogleDrive", return_value="mocked_drive"
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
        "gskeleton.drive_etl.gspread.service_account",
        return_value="mocked_gspread_client",
    )
    mocker.patch(
        "gskeleton.drive_etl.ServiceAccountCredentials.from_json_keyfile_name",
        return_value="mocked_drive_credentials",
    )

    etl = DriveETL()
    assert etl.mime_types.get("yaml") is not None
    etl.service_auth("test_path.json")
    my_list = [
        {
            "id": "1key",
            "title": "File1.yaml",
            "mimeType": "application/x-yaml",
            "modifiedDate": "1",
        },
        {
            "id": "2key",
            "title": "File2.yaml",
            "mimeType": "application/x-yaml",
            "modifiedDate": "2",
        },
        {
            "id": "3key",
            "title": "File3.json",
            "mimeType": "application/json",
            "modifiedDate": "3",
        },
    ]
    list_file = MockedListFile(my_list)
    mocker.patch(
        "gskeleton.drive_etl.GoogleDrive.ListFile", return_value=list_file
    )
    folder = GFolder(key="test_folder_key")
    file1 = GFile(key="1key")
    file2 = GFile(key="2key")
    file3 = GFile(key="3key")

    files = etl._select_files(GFileSelector(folder=folder))
    assert files == [file1, file2, file3]

    files = etl._select_files(GFileSelector(folder=folder, extension="yaml"))
    assert files == [file1, file2]

    files = etl._select_files(GFileSelector(folder=folder, extension="json"))
    assert files == [file3]

    mocker.patch(
        "gskeleton.drive_etl.GoogleDrive.CreateFile",
        return_value=MockedCreateFile(),
    )
    path = etl._download_drive_file(file2)
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
