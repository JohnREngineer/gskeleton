import gskeleton


def test_gskeleton(mocker):
    mocker.patch(
        "gskeleton.driveetl.gspread.authorize",
        return_value="test_gspread_credentials",
    )
    gs = gskeleton.authorize("test_credentials")
    assert gs.gc == "test_gspread_credentials"
    assert gs.drive.auth.credentials == "test_credentials"

    gs.run_etl("test_config_key")
    assert gs.config_key == "test_config_key"
