from gsqletl import __version__
from gsqletl.gsqletl import DriveLocation, GSkeleton
from gsqletl.toy import User, multiply_two_numbers


def test_version():
    assert __version__ == "0.1.0"


def test_gskeleton_init():
    skel = GSkeleton('asdf1234')
    assert skel.settings_location == DriveLocation(key='asdf1234')


def test_user():
    user = User(id="123")
    assert user.id == 123
    assert user.name == "Jane Doe"


def test_multiply_two_numbers():
    result = multiply_two_numbers(2, 3)
    assert result == 6
