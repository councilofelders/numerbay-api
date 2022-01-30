import os
import pytest
from click.testing import CliRunner
from unittest.mock import patch

from numerbay import cli


@pytest.fixture(scope="function", name="login")
def login():
    os.environ["NUMERBAY_USERNAME"] = "foo"
    os.environ["NUMERBAY_PASSWORD"] = "bar"
    yield None
    # teardown
    del os.environ["NUMERBAY_USERNAME"]
    del os.environ["NUMERBAY_PASSWORD"]


@patch("numerbay.NumerBay.get_account")
def test_account(mocked, login):
    result = CliRunner().invoke(cli.account)
    # just testing if calling works fine
    assert result.exit_code == 0


@patch("numerbay.NumerBay.get_my_listings")
def test_listings(mocked, login):
    result = CliRunner().invoke(cli.listings)
    # just testing if calling works fine
    assert result.exit_code == 0


@patch("numerbay.NumerBay.get_my_orders")
def test_orders(mocked, login):
    result = CliRunner().invoke(cli.orders)
    # just testing if calling works fine
    assert result.exit_code == 0


@patch("numerbay.NumerBay.get_my_sales")
def test_sales(mocked, login):
    result = CliRunner().invoke(cli.sales)
    # just testing if calling works fine
    assert result.exit_code == 0


@patch("numerbay.NumerBay.upload_artifact")
def test_submit(mocked, login, tmpdir):
    path = tmpdir.join("somefilepath")
    path.write("content")
    result = CliRunner().invoke(cli.submit, [str(path), "--product_id", 2])
    # just testing if calling works fine
    assert result.exit_code == 0


@patch("numerbay.NumerBay.download_artifact")
def test_download(mocked, login, tmpdir):
    result = CliRunner().invoke(cli.download, ["--product_id", 2])
    # just testing if calling works fine
    assert result.exit_code == 0


def test_version():
    result = CliRunner().invoke(cli.version)
    # just testing if calling works fine
    assert result.exit_code == 0
