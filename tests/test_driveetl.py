from gskeleton.driveetl import DriveETL, DriveLocation


def test_driveetl_init(mocker):
    mocker.patch(
        "gskeleton.driveetl.gspread.authorize",
        return_value="test_gspread_credentials",
    )
    etl = DriveETL("test_settings_key", "test_credentials")
    assert etl.settings_location == DriveLocation(key="test_settings_key")
    assert etl.gc == "test_gspread_credentials"
    assert etl.drive.auth.credentials == "test_credentials"
