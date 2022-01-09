import pytest
import datetime
import os
from dateutil.tz import tzutc
import responses
import requests
import decimal
from numerbay import utils


def test_parse_datetime_string():
    s = "2017-12-24T20:48:25.90349Z"
    t = datetime.datetime(2017, 12, 24, 20, 48, 25, 903490, tzinfo=tzutc())
    assert utils.parse_datetime_string(s) == t
    assert utils.parse_datetime_string(None) is None


def test_parse_float_string():
    assert utils.parse_float_string(None) is None
    assert utils.parse_float_string("") is None
    assert utils.parse_float_string("1.23") == decimal.Decimal("1.23")
    assert utils.parse_float_string("12") == decimal.Decimal("12.0")
    assert utils.parse_float_string("1,000.0") == decimal.Decimal("1000.0")
    assert utils.parse_float_string("0.4") == decimal.Decimal("0.4")


def test_replace():
    d = None
    assert utils.replace(d, "a", float) is None
    # empty dict
    d = {}
    assert not utils.replace(d, "a", float)
    # normal case
    d = {"a": "1"}
    utils.replace(d, "a", float)
    assert d["a"] == 1.0


@responses.activate
def test_download_file(tmpdir):
    url = "https://someurl"
    responses.add(responses.GET, url)

    # convert to string to make python<3.6 happy
    path = str(tmpdir.join("somefilepath"))
    utils.download_file("https://someurl", path)
    assert os.path.exists(path)
