import nacl.utils
from nacl.public import PrivateKey, SealedBox
import pytest
import responses

import pandas as pd

import numerbay
from numerbay import API_ENDPOINT_URL


@pytest.fixture(scope="function", name="api")
def api_fixture():
    api = numerbay.NumerBay(verbosity="DEBUG")
    return api


def test_NumerBay():
    # invalid log level should raise
    with pytest.raises(AttributeError):
        numerbay.NumerBay(verbosity="FOO")


@responses.activate
def test_get_account(api):
    data = {"detail": "Could not validate credentials"}
    responses.add(responses.GET, f"{API_ENDPOINT_URL}/users/me", json=data, status=403)

    with pytest.raises(ValueError) as excinfo:
        api.get_account()
    assert str(excinfo.value) == "Could not validate credentials"
    responses.reset()

    api.user_id = 2
    api.token = "Token"
    data = {
        "email": "me@example.com",
        "is_active": True,
        "is_superuser": False,
        "username": "myusername",
        "public_address": "0xmymetamaskaddressdde80ca30248e7a8890cacb",
        "id": 2,
        "numerai_api_key_public_id": "MYNUMERAIAPIKEYRCXBVB66ACTSLDR53",
        "numerai_api_key_can_upload_submission": True,
        "numerai_api_key_can_stake": True,
        "numerai_api_key_can_read_submission_info": True,
        "numerai_api_key_can_read_user_info": True,
        "numerai_wallet_address": "0x000000000000000000000000mynumeraiaddress",
        "models": [],
    }
    responses.add(responses.GET, f"{API_ENDPOINT_URL}/users/me", json=data)
    account = api.get_account()
    assert isinstance(account, dict)
    assert account.get("username") == "myusername"


@responses.activate
def test_get_my_listings(api):
    api.user_id = 2
    api.token = "Token"
    data = {
        "total": 1,
        "data": [
            {
                "avatar": "https://example.com/example.jpg",
                "description": "Product description",
                "is_active": True,
                "is_ready": False,
                "expiration_round": None,
                "total_num_sales": 0,
                "last_sale_price": None,
                "last_sale_price_delta": None,
                "featured_products": None,
                "id": 108,
                "name": "mymodel",
                "sku": "numerai-predictions-mymodel",
                "category": {
                    "name": "Predictions",
                    "slug": "numerai-predictions",
                    "tournament": 8,
                    "is_per_round": True,
                    "is_submission": True,
                    "id": 3,
                    "items": [],
                    "parent": {},
                },
                "owner": {"id": 2, "username": "myusername"},
                "options": [
                    {
                        "id": 6,
                        "is_on_platform": True,
                        "third_party_url": None,
                        "description": None,
                        "quantity": 1,
                        "price": 1,
                        "currency": "NMR",
                        "wallet": None,
                        "chain": None,
                        "stake_limit": None,
                        "mode": "file",
                        "is_active": True,
                        "coupon": None,
                        "coupon_specs": None,
                        "special_price": None,
                        "applied_coupon": None,
                        "product_id": 108,
                    }
                ],
            }
        ],
    }
    responses.add(
        responses.POST, f"{API_ENDPOINT_URL}/products/search-authenticated", json=data
    )
    listings = api.get_my_listings()
    assert listings[0]["name"] == data["data"][0]["name"]


@responses.activate
def test_get_my_orders(api):
    api.user_id = 2
    api.token = "Token"
    data = {
        "total": 1,
        "data": [
            {
                "date_order": "2021-12-25T06:34:58.047278",
                "round_order": 296,
                "quantity": 1,
                "price": 9,
                "currency": "NMR",
                "mode": "file",
                "stake_limit": None,
                "submit_model_id": None,
                "submit_model_name": None,
                "submit_state": None,
                "chain": "ethereum",
                "from_address": "0x00000000000000000000000000000fromaddress",
                "to_address": "0x0000000000000000000000000000000toaddress",
                "transaction_hash": "0x09bd2a0f814a745f62cb35f1a41dd18208fb653210ff677e946747a20e5abcdef",
                "state": "confirmed",
                "applied_coupon_id": 1,
                "coupon": None,
                "coupon_specs": None,
                "id": 126,
                "product": {},
                "buyer": {"id": 2, "username": "myusername"},
            }
        ],
    }
    responses.add(responses.POST, f"{API_ENDPOINT_URL}/orders/search", json=data)
    orders = api.get_my_orders()
    assert orders[0]["buyer"]["username"] == data["data"][0]["buyer"]["username"]


@responses.activate
def test_get_my_sales(api):
    api.user_id = 2
    api.token = "Token"
    data = {
        "total": 1,
        "data": [
            {
                "date_order": "2021-12-25T06:34:58.047278",
                "round_order": 296,
                "quantity": 1,
                "price": 9,
                "currency": "NMR",
                "mode": "file",
                "stake_limit": None,
                "submit_model_id": None,
                "submit_model_name": None,
                "submit_state": None,
                "chain": "ethereum",
                "from_address": "0x00000000000000000000000000000fromaddress",
                "to_address": "0x0000000000000000000000000000000toaddress",
                "transaction_hash": "0x09bd2a0f814a745f62cb35f1a41dd18208fb653210ff677e946747a20e5abcdef",
                "state": "confirmed",
                "applied_coupon_id": 1,
                "coupon": None,
                "coupon_specs": None,
                "id": 126,
                "product": {},
                "buyer": {"id": 2, "username": "someusername"},
            }
        ],
    }
    responses.add(responses.POST, f"{API_ENDPOINT_URL}/orders/search", json=data)
    orders = api.get_my_sales()
    assert orders[0]["buyer"]["username"] == data["data"][0]["buyer"]["username"]


@responses.activate
def test_upload_artifact(api, tmpdir):
    api.user_id = 2
    api.token = "Token"

    # mock product search
    product_id = 2
    data = {
        "total": 1,
        "data": [
            {
                "id": product_id,
                "name": "mymodel",
                "sku": "numerai-predictions-mymodel",
            }
        ],
    }
    responses.add(
        responses.POST, f"{API_ENDPOINT_URL}/products/search-authenticated", json=data
    )

    artifact_id = 3
    data = {"url": "https://uploadurl", "id": artifact_id}
    responses.add(
        responses.POST,
        f"{API_ENDPOINT_URL}/products/{product_id}/artifacts/generate-upload-url",
        json=data,
    )
    responses.add(responses.PUT, "https://uploadurl")
    data = {"id": artifact_id}
    responses.add(
        responses.POST,
        f"{API_ENDPOINT_URL}/products/{product_id}/artifacts/{artifact_id}/validate-upload",
        json=data,
    )

    path = tmpdir.join("somefilepath")
    path.write("content")

    # upload file with product_id
    artifact = api.upload_artifact(str(path), product_id=product_id)
    assert artifact["id"] == artifact_id

    # upload file with product_full_name
    data = {
        "total": 1,
        "data": [
            {
                "id": product_id,
                "name": "mymodel",
                "sku": "numerai-predictions-mymodel",
            }
        ],
    }
    responses.add(
        responses.POST, f"{API_ENDPOINT_URL}/products/search-authenticated", json=data
    )
    artifact = api.upload_artifact(
        str(path), product_full_name="numerai-predictions-mymodel"
    )
    assert artifact["id"] == artifact_id


@responses.activate
def test_upload_artifact_df(api):
    api.user_id = 2
    api.token = "Token"

    # mock product search
    product_id = 2
    data = {
        "total": 1,
        "data": [
            {
                "id": product_id,
                "name": "mymodel",
                "sku": "numerai-predictions-mymodel",
            }
        ],
    }
    responses.add(
        responses.POST, f"{API_ENDPOINT_URL}/products/search-authenticated", json=data
    )

    artifact_id = 3
    data = {"url": "https://uploadurl", "id": artifact_id}
    responses.add(
        responses.POST,
        f"{API_ENDPOINT_URL}/products/{product_id}/artifacts/generate-upload-url",
        json=data,
    )
    responses.add(responses.PUT, "https://uploadurl")
    data = {"id": artifact_id}
    responses.add(
        responses.POST,
        f"{API_ENDPOINT_URL}/products/{product_id}/artifacts/{artifact_id}/validate-upload",
        json=data,
    )

    df = pd.DataFrame.from_dict({"id": [], "prediction": []})

    # upload df with product_id
    artifact = api.upload_artifact(df=df, product_id=product_id)
    assert artifact["id"] == artifact_id

    # upload df with product_full_name
    data = {
        "total": 1,
        "data": [
            {
                "id": product_id,
                "name": "mymodel",
                "sku": "numerai-predictions-mymodel",
            }
        ],
    }
    responses.add(
        responses.POST, f"{API_ENDPOINT_URL}/products/search-authenticated", json=data
    )
    artifact = api.upload_artifact(
        df=df, product_full_name="numerai-predictions-mymodel"
    )
    assert artifact["id"] == artifact_id


@responses.activate
def test_upload_encrypted_artifact(api, tmpdir):
    key_pair = nacl.public.PrivateKey.generate()
    public_key = key_pair.public_key.encode(encoder=nacl.encoding.Base64Encoder)

    api.user_id = 2
    api.token = "Token"

    # mock product search
    product_id = 2
    data = {
        "total": 1,
        "data": [
            {
                "id": product_id,
                "name": "mymodel",
                "sku": "numerai-predictions-mymodel",
                "use_encryption": True,
            }
        ],
    }
    responses.add(
        responses.POST, f"{API_ENDPOINT_URL}/products/search-authenticated", json=data
    )

    # mock order search
    data = {
        "total": 1,
        "data": [
            {
                "date_order": "2021-12-25T06:34:58.047278",
                "round_order": 296,
                "quantity": 1,
                "price": 9,
                "currency": "NMR",
                "mode": "file",
                "stake_limit": None,
                "submit_model_id": None,
                "submit_model_name": None,
                "submit_state": None,
                "chain": "ethereum",
                "from_address": "0x00000000000000000000000000000fromaddress",
                "to_address": "0x0000000000000000000000000000000toaddress",
                "transaction_hash": "0x09bd2a0f814a745f62cb35f1a41dd18208fb653210ff677e946747a20e5abcdef",
                "state": "confirmed",
                "applied_coupon_id": 1,
                "coupon": None,
                "coupon_specs": None,
                "id": 126,
                "product": {"id": product_id},
                "buyer": {"id": 2, "username": "myusername"},
                "buyer_public_key": public_key.decode("ascii"),
            }
        ],
    }
    responses.add(responses.POST, f"{API_ENDPOINT_URL}/orders/search", json=data)

    artifact_id = "abc"
    data = {"url": "https://uploadurl", "id": artifact_id}
    responses.add(
        responses.POST, f"{API_ENDPOINT_URL}/artifacts/generate-upload-url", json=data
    )
    responses.add(responses.PUT, "https://uploadurl")
    data = {"id": artifact_id}
    responses.add(
        responses.POST,
        f"{API_ENDPOINT_URL}/artifacts/{artifact_id}/validate-upload",
        json=data,
    )

    path = tmpdir.join("somefilepath")
    path.write("content")

    # upload file with product_id
    artifacts = api.upload_artifact(str(path), product_id=product_id)
    assert artifacts[0]["id"] == artifact_id

    # upload df with product_id
    df = pd.DataFrame.from_dict({"id": [], "prediction": []})
    artifacts = api.upload_artifact(df=df, product_id=product_id)
    assert artifacts[0]["id"] == artifact_id

    # upload file with product_full_name
    data = {
        "total": 1,
        "data": [
            {
                "id": product_id,
                "name": "mymodel",
                "sku": "numerai-predictions-mymodel",
                "use_encryption": True,
            }
        ],
    }
    responses.add(
        responses.POST, f"{API_ENDPOINT_URL}/products/search-authenticated", json=data
    )
    artifacts = api.upload_artifact(
        str(path), product_full_name="numerai-predictions-mymodel"
    )
    assert artifacts[0]["id"] == artifact_id


@responses.activate
def test_upload_numerai_submission(api, tmpdir):
    key_pair = nacl.public.PrivateKey.generate()
    public_key = key_pair.public_key.encode(encoder=nacl.encoding.Base64Encoder)

    api.user_id = 2
    api.token = "Token"

    # mock product search
    product_id = 2
    data = {
        "total": 1,
        "data": [
            {
                "id": product_id,
                "name": "mymodel",
                "sku": "numerai-predictions-mymodel",
                "use_encryption": True,
            }
        ],
    }
    responses.add(
        responses.POST, f"{API_ENDPOINT_URL}/products/search-authenticated", json=data
    )

    # mock order search
    data = {
        "total": 1,
        "data": [
            {
                "date_order": "2021-12-25T06:34:58.047278",
                "round_order": 296,
                "quantity": 1,
                "price": 9,
                "currency": "NMR",
                "mode": "stake",
                "stake_limit": None,
                "submit_model_id": "some_model_id",
                "submit_model_name": None,
                "submit_state": None,
                "chain": "ethereum",
                "from_address": "0x00000000000000000000000000000fromaddress",
                "to_address": "0x0000000000000000000000000000000toaddress",
                "transaction_hash": "0x09bd2a0f814a745f62cb35f1a41dd18208fb653210ff677e946747a20e5abcdef",
                "state": "confirmed",
                "applied_coupon_id": 1,
                "coupon": None,
                "coupon_specs": None,
                "id": 126,
                "product": {"id": product_id},
                "buyer": {"id": 2, "username": "myusername"},
                "buyer_public_key": public_key.decode("ascii"),
            }
        ],
    }
    responses.add(responses.POST, f"{API_ENDPOINT_URL}/orders/search", json=data)

    artifact_id = "abc"
    data = {"url": "https://numerai_uploadurl", "id": artifact_id}
    responses.add(
        responses.POST, f"{API_ENDPOINT_URL}/artifacts/generate-upload-url", json=data
    )
    responses.add(responses.PUT, "https://numerai_uploadurl")
    data = {"id": artifact_id, "is_numerai_direct": True}
    responses.add(
        responses.POST,
        f"{API_ENDPOINT_URL}/artifacts/{artifact_id}/validate-upload",
        json=data,
    )

    path = tmpdir.join("somefilepath")
    path.write("content")

    # upload file with product_id
    artifacts = api.upload_artifact(str(path), product_id=product_id)
    assert artifacts[0]["id"] == artifact_id
    assert artifacts[0]["is_numerai_direct"]

    # upload df with product_id
    df = pd.DataFrame.from_dict({"id": [], "prediction": []})
    artifacts = api.upload_artifact(df=df, product_id=product_id)
    assert artifacts[0]["id"] == artifact_id
    assert artifacts[0]["is_numerai_direct"]

    # upload file with product_full_name
    data = {
        "total": 1,
        "data": [
            {
                "id": product_id,
                "name": "mymodel",
                "sku": "numerai-predictions-mymodel",
                "use_encryption": True,
            }
        ],
    }
    responses.add(
        responses.POST, f"{API_ENDPOINT_URL}/products/search-authenticated", json=data
    )
    artifacts = api.upload_artifact(
        str(path), product_full_name="numerai-predictions-mymodel"
    )
    assert artifacts[0]["id"] == artifact_id


@responses.activate
def test_download_artifact(api, tmpdir):
    # mock order search
    data = {
        "total": 1,
        "data": [
            {
                "date_order": "2021-12-25T06:34:58.047278",
                "round_order": 296,
                "quantity": 1,
                "price": 9,
                "currency": "NMR",
                "mode": "file",
                "stake_limit": None,
                "submit_model_id": None,
                "submit_model_name": None,
                "submit_state": None,
                "chain": "ethereum",
                "from_address": "0x00000000000000000000000000000fromaddress",
                "to_address": "0x0000000000000000000000000000000toaddress",
                "transaction_hash": "0x09bd2a0f814a745f62cb35f1a41dd18208fb653210ff677e946747a20e5abcdef",
                "state": "confirmed",
                "applied_coupon_id": 1,
                "coupon": None,
                "coupon_specs": None,
                "id": 126,
                "product": {"id": 4},
                "buyer": {"id": 2, "username": "myusername"},
            }
        ],
    }
    responses.add(responses.POST, f"{API_ENDPOINT_URL}/orders/search", json=data)

    # mock product search
    product_id = 4

    data = {
        "total": 1,
        "data": [
            {
                "id": product_id,
                "name": "somemodel",
                "sku": "numerai-predictions-somemodel",
            }
        ],
    }
    responses.add(
        responses.POST, f"{API_ENDPOINT_URL}/products/search-authenticated", json=data
    )

    api.user_id = 2
    api.token = "Token"
    api.show_progress_bars = False

    artifact_id = 3
    data = "https://downloadurl"
    responses.add(
        responses.GET,
        f"{API_ENDPOINT_URL}/products/{product_id}/artifacts/{artifact_id}/generate-download-url",
        json=data,
    )
    responses.add(responses.GET, "https://downloadurl")

    # download file with product_id and artifact_id
    api.download_artifact(
        dest_path=tmpdir.join("artifact.csv"),
        product_id=product_id,
        artifact_id=artifact_id,
    )

    # download file with product_full_name only
    data = {
        "total": 1,
        "data": [
            {
                "id": artifact_id,
            }
        ],
    }
    responses.add(
        responses.GET, f"{API_ENDPOINT_URL}/products/{product_id}/artifacts", json=data
    )

    api.download_artifact(
        dest_path=tmpdir.join("artifact.csv"),
        product_full_name="numerai-predictions-somemodel",
    )


@responses.activate
def test_download_artifact_no_active_artifact(api):
    # mock order search
    data = {
        "total": 1,
        "data": [
            {
                "date_order": "2021-12-25T06:34:58.047278",
                "round_order": 296,
                "quantity": 1,
                "price": 9,
                "currency": "NMR",
                "mode": "file",
                "stake_limit": None,
                "submit_model_id": None,
                "submit_model_name": None,
                "submit_state": None,
                "chain": "ethereum",
                "from_address": "0x00000000000000000000000000000fromaddress",
                "to_address": "0x0000000000000000000000000000000toaddress",
                "transaction_hash": "0x09bd2a0f814a745f62cb35f1a41dd18208fb653210ff677e946747a20e5abcdef",
                "state": "confirmed",
                "applied_coupon_id": 1,
                "coupon": None,
                "coupon_specs": None,
                "id": 126,
                "product": {"id": 4},
                "buyer": {"id": 2, "username": "myusername"},
            }
        ],
    }
    responses.add(responses.POST, f"{API_ENDPOINT_URL}/orders/search", json=data)

    api.user_id = 2
    api.token = "Token"
    api.show_progress_bars = False

    product_id = 4

    # download file with product_full_name only
    data = {
        "total": 0,
        "data": [
            {
                "id": product_id,
                "name": "somemodel",
                "sku": "numerai-predictions-somemodel",
            }
        ],
    }
    responses.add(
        responses.POST, f"{API_ENDPOINT_URL}/products/search-authenticated", json=data
    )
    data = {"total": 0, "data": []}
    responses.add(
        responses.GET, f"{API_ENDPOINT_URL}/products/{product_id}/artifacts", json=data
    )

    with pytest.raises(ValueError) as err:
        api.download_artifact(
            dest_path="artifact.csv", product_full_name="numerai-predictions-somemodel"
        )
    assert str(err.value).startswith("Failed to resolve artifact")


@responses.activate
def test_download_encrypted_artifact(api, tmpdir):
    key_pair = nacl.public.PrivateKey.generate()
    public_key = key_pair.public_key.encode(encoder=nacl.encoding.Base64Encoder)
    private_key = key_pair.encode(encoder=nacl.encoding.Base64Encoder)

    # mock order search
    artifact_id = "abc"
    data = {
        "total": 1,
        "data": [
            {
                "date_order": "2021-12-25T06:34:58.047278",
                "round_order": 296,
                "quantity": 1,
                "price": 9,
                "currency": "NMR",
                "mode": "file",
                "stake_limit": None,
                "submit_model_id": None,
                "submit_model_name": None,
                "submit_state": None,
                "chain": "ethereum",
                "from_address": "0x00000000000000000000000000000fromaddress",
                "to_address": "0x0000000000000000000000000000000toaddress",
                "transaction_hash": "0x09bd2a0f814a745f62cb35f1a41dd18208fb653210ff677e946747a20e5abcdef",
                "state": "confirmed",
                "applied_coupon_id": 1,
                "coupon": None,
                "coupon_specs": None,
                "id": 126,
                "product": {"id": 4},
                "buyer": {"id": 2, "username": "myusername"},
                "buyer_public_key": public_key.decode("ascii"),
                "artifacts": [{"id": artifact_id, "state": "active"}],
            }
        ],
    }
    responses.add(responses.POST, f"{API_ENDPOINT_URL}/orders/search", json=data)

    # mock product search
    product_id = 4

    data = {
        "total": 1,
        "data": [
            {
                "id": product_id,
                "name": "somemodel",
                "sku": "numerai-predictions-somemodel",
                "use_encryption": True,
            }
        ],
    }
    responses.add(
        responses.POST, f"{API_ENDPOINT_URL}/products/search-authenticated", json=data
    )

    api.user_id = 2
    api.token = "Token"
    api.show_progress_bars = False

    data = "https://downloadurl"
    content = b"hello"
    responses.add(
        responses.GET,
        f"{API_ENDPOINT_URL}/artifacts/{artifact_id}/generate-download-url",
        json=data,
    )
    responses.add(
        responses.GET,
        "https://downloadurl",
        body=SealedBox(key_pair.public_key).encrypt(content),
    )

    # download file with product_id and artifact_id
    api.download_artifact(
        dest_path=tmpdir.join("encrypted_artifact_1.csv"),
        product_id=product_id,
        artifact_id=artifact_id,
        key_base64=private_key.decode("ascii"),
    )

    with open(tmpdir.join("encrypted_artifact_1.csv"), "rb") as file:
        assert file.read() == content

    # download file with product_full_name only
    data = {
        "total": 1,
        "data": [
            {
                "id": artifact_id,
            }
        ],
    }
    responses.add(
        responses.GET, f"{API_ENDPOINT_URL}/products/{product_id}/artifacts", json=data
    )

    api.download_artifact(
        dest_path=tmpdir.join("encrypted_artifact_2.csv"),
        product_full_name="numerai-predictions-somemodel",
        key_base64=private_key.decode("ascii"),
    )

    with open(tmpdir.join("encrypted_artifact_2.csv"), "rb") as file:
        assert file.read() == content
