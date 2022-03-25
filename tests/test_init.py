import gskeleton


def test_gskeleton(mocker):
    mocker.patch(
        "gskeleton.driveetl.gspread.authorize",
        return_value="test_gspread_credentials",
    )
    gs = gskeleton.authorize("test_credentials")
    assert gs.gspread_client == "test_gspread_credentials"
    assert gs.drive.auth.credentials == "test_credentials"
