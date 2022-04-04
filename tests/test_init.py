import gskeleton
from gskeleton.drive_etl import DriveETL


def test_gskeleton(mocker):
    mock_service_auth = mocker.patch(
        "gskeleton.drive_etl.DriveETL.service_auth",
    )
    test_path = "test/test_path.json"
    gs = gskeleton.authorize(test_path)
    assert isinstance(gs, DriveETL)
    assert mock_service_auth.call_args[0][0] == test_path
