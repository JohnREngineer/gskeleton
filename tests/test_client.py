from gskeleton.driveetl import DriveETL, DriveLocation


def test_driveetl_init():
    skel = DriveETL("asdf1234")
    assert skel.settings_location == DriveLocation(key="asdf1234")
