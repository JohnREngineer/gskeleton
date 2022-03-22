from gskeleton.client import DriveLocation, Client


def test_client_init():
    skel = Client('asdf1234')
    assert skel.settings_location == DriveLocation(key='asdf1234')
