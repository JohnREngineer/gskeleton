import gskeleton
from gskeleton.driveetl import DriveLocation


def test_gskeleton(mocker):
    mocker.patch(
        "gskeleton.driveetl.gspread.authorize",
        return_value="test_gspread_credentials",
    )
    gs = gskeleton.authorize("test_credentials")
    assert gs.gc == "test_gspread_credentials"
    assert gs.drive.auth.credentials == "test_credentials"

    gs.run_etl("test_settings_key")
    assert gs.settings_location == DriveLocation(key="test_settings_key")


# def test_driveetl_init(mocker):
#     mocker.patch(
#         'gskeleton.driveetl.gspread.authorize',
#         return_value='test_gspread_credentials',
#     )
#     etl = DriveETL('test_settings_key', 'test_credentials')
#     assert etl.settings_location == DriveLocation(key='test_settings_key')
#     assert etl.gc == 'test_gspread_credentials'
#     assert etl.drive.auth.credentials == 'test_credentials'
