"""NumerBay API module

Install via:
```commandline
pip install numerbay
```
"""

import os
import logging
from typing import Dict, List, Union
from io import BytesIO

import pandas as pd
import requests

from numerbay import utils

API_ENDPOINT_URL = "https://numerbay.ai/backend-api/v1"


class NumerBay:
    """Wrapper around the NumerBay API"""

    def __init__(
        self, username=None, password=None, verbosity="INFO", show_progress_bars=True
    ):
        """
        initialize NumerBay API wrapper for Python

        Args:
            username (str): your NumerBay username
            password (str): your NumerBay password
            verbosity (str): indicates what level of messages should be
                displayed. valid values are "debug", "info", "warning",
                "error" and "critical"
            show_progress_bars (bool): flag to turn of progress bars
        """

        # set up logging
        self.logger = logging.getLogger(__name__)
        numeric_log_level = getattr(logging, verbosity.upper())
        log_format = "%(asctime)s %(levelname)s %(name)s: %(message)s"
        logging.basicConfig(format=log_format, level=numeric_log_level)

        self._login(username, password)

        self.user_id = None
        if self.token is not None:
            account = self.get_account()
            self.user_id = account["id"]

        self.show_progress_bars = show_progress_bars

    def _login(self, username=None, password=None):
        # check env variables if not set
        if not username:
            username = os.getenv("NUMERBAY_USERNAME")
        if not password:
            password = os.getenv("NUMERBAY_PASSWORD")

        if username and password:
            self.token = utils.post_with_err_handling(
                f"{API_ENDPOINT_URL}/login/access-token",
                body={"username": username, "password": password},
                headers=None,
            ).get("access_token", None)
        elif not username and not password:
            self.token = None
        else:
            self.logger.warning("You need to supply both a username and a password.")
            self.token = None

    def _handle_call_error(self, errors):
        if isinstance(errors, list):
            for error in errors:
                if "message" in error:
                    msg = error["message"]
                    self.logger.error(msg)
        elif isinstance(errors, dict):
            if "detail" in errors:
                msg = errors["detail"]
                self.logger.error(msg)
        elif isinstance(errors, str):
            msg = errors
            self.logger.error(errors)
        return msg

    def get_account(self) -> Dict:
        """Get all information about your account!

        Returns:
            dict: user information including the following fields:

                * email (`str`)
                * id (`int`)
                * is_active (`bool`)
                * is_superuser (`bool`)
                * public_address (`str`)
                * username (`str`)
                * numerai_api_key_public_id (`str`)
                * numerai_api_key_can_read_user_info (`bool`)
                * numerai_api_key_can_read_submission_info (`bool`)
                * numerai_api_key_can_upload_submission (`bool`)
                * numerai_api_key_can_stake (`bool`)
                * numerai_wallet_address (`str`)
                * models (`list`) each with the following fields:
                  * id (`str`)
                  * name (`str`)
                  * tournament (`int`)
                  * start_date (`datetime`)
                * coupons (`list`)

        Example:
            ```python
            api = NumerBay(username="..", password="..")
            api.get_account()
            ```
            ```
            {
                "email":"me@example.com",
                "is_active":True,
                "is_superuser":False,
                "username":"myusername",
                "public_address":"0xmymetamaskaddressdde80ca30248e7a8890cacb",
                "id":2,
                "numerai_api_key_public_id":"MYNUMERAIAPIKEYRCXBVB66ACTSLDR53",
                "numerai_api_key_can_upload_submission":True,
                "numerai_api_key_can_stake":True,
                "numerai_api_key_can_read_submission_info":True,
                "numerai_api_key_can_read_user_info":True,
                "numerai_wallet_address":"0x000000000000000000000000mynumeraiaddress",
                "models":[{
                    "id":"xxxxxxxx-xxxx-xxxx-xxxx-411487a4d64a",
                    "name":"mymodel",
                    "tournament":8,
                    "start_date":"2021-03-22T17:44:50"
                }, ..],
                "coupons":[..]
             }
             ```
        """
        data = utils.get_with_err_handling(
            f"{API_ENDPOINT_URL}/users/me",
            params=None,
            headers={"Authorization": f"Bearer {self.token}"},
        )

        if data and "detail" in data:
            err = self._handle_call_error(data["detail"])
            # fail!
            raise ValueError(err)
        return data

    def get_my_orders(self) -> List:
        """Get all your orders.
        Returns:
            list: List of dicts with the following structure:

                 * date_order (`datetime`)
                 * round_order (`int`)
                 * quantity (`int`)
                 * price (`decimal.Decimal`)
                 * currency (`str`)
                 * mode (`str`)
                 * stake_limit (`decimal.Decimal`)
                 * submit_model_id (`str`)
                 * submit_model_name (`str`)
                 * submit_state (`str`)
                 * chain (`str`)
                 * from_address (`str`)
                 * to_address (`str`)
                 * transaction_hash (`str`)
                 * state (`str`)
                 * applied_coupon_id (`int`)
                 * coupon (`bool`)
                 * coupon_specs (`dict`)
                 * id (`int`)
                 * product (`dict`)
                 * buyer (`dict`)
                 * buyer_public_key (`str`)
                 * artifacts (`list`)
        Example:
            ```python
            api = NumerBay(username="..", password="..")
            api.get_my_orders()
            ```
            ```
            [{
                "date_order":"2021-12-25T06:34:58.047278",
                "round_order":296,
                "quantity":1,
                "price":9,
                "currency":"NMR",
                "mode":"file",
                "stake_limit":None,
                "submit_model_id":None,
                "submit_model_name":None,
                "submit_state":None,
                "chain":"ethereum",
                "from_address":"0x00000000000000000000000000000fromaddress",
                "to_address":"0x0000000000000000000000000000000toaddress",
                "transaction_hash":"0x09bd2a0f814a745...7a20e5abcdef",
                "state":"confirmed",
                "applied_coupon_id":1,
                "coupon":None,
                "coupon_specs":None,
                "id":126,
                "product":{..},
                "buyer":{
                    "id":2,
                    "username":"myusername"
                },
                "buyer_public_key":"yqKFQtzss8vRL7devlvZY70v5WUrS3BfKfPFzTnYit4=",
                "artifacts":[...]
            }, ...]
            ```
        """
        data = utils.post_with_err_handling(
            f"{API_ENDPOINT_URL}/orders/search",
            json={"role": "buyer"},
            headers={"Authorization": f"Bearer {self.token}"},
        )

        if data and "detail" in data:
            err = self._handle_call_error(data["detail"])
            # fail!
            raise ValueError(err)
        return data.get("data", [])

    def get_my_sales(self, active_only: bool = False) -> List:
        """Get all your sales (including pending and expired sales).
        Args:
            active_only (bool, optional): whether to fetch only active sale orders
        Returns:
            list: List of dicts with the following structure:

                 * date_order (`datetime`)
                 * round_order (`int`)
                 * quantity (`int`)
                 * price (`decimal.Decimal`)
                 * currency (`str`)
                 * mode (`str`)
                 * stake_limit (`decimal.Decimal`)
                 * submit_model_id (`str`)
                 * submit_model_name (`str`)
                 * submit_state (`str`)
                 * chain (`str`)
                 * from_address (`str`)
                 * to_address (`str`)
                 * transaction_hash (`str`)
                 * state (`str`)
                 * applied_coupon_id (`int`)
                 * coupon (`bool`)
                 * coupon_specs (`dict`)
                 * id (`int`)
                 * product (`dict`)
                 * buyer (`dict`)
                 * buyer_public_key (`str`)
                 * artifacts (`list`)
        Example:
            ```python
            api = NumerBay(username="..", password="..")
            api.get_my_sales()
            ```
            ```
            [{
                "date_order":"2021-12-25T06:34:58.047278",
                "round_order":296,
                "quantity":1,
                "price":9,
                "currency":"NMR",
                "mode":"file",
                "stake_limit":None,
                "submit_model_id":None,
                "submit_model_name":None,
                "submit_state":None,
                "chain":"ethereum",
                "from_address":"0x00000000000000000000000000000fromaddress",
                "to_address":"0x0000000000000000000000000000000toaddress",
                "transaction_hash":"0x09bd2a0f814a745...7a20e5abcdef",
                "state":"confirmed",
                "applied_coupon_id":1,
                "coupon":None,
                "coupon_specs":None,
                "id":126,
                "product":{..},
                "buyer":{
                    "id":2,
                    "username":"someusername"
                },
                "buyer_public_key":"yqKFQtzss8vRL7devlvZY70v5WUrS3BfKfPFzTnYit4=",
                "artifacts":[...]
            }, ...]
            ```
        """
        json = {"role": "seller"}
        if active_only:
            json["filters"] = {"active": True}
        data = utils.post_with_err_handling(
            f"{API_ENDPOINT_URL}/orders/search",
            json=json,
            headers={"Authorization": f"Bearer {self.token}"},
        )

        if data and "detail" in data:
            err = self._handle_call_error(data["detail"])
            # fail!
            raise ValueError(err)
        return data.get("data", [])

    def _upload_auth(self, file_path: str, product_id: int) -> Dict[str, str]:
        data = utils.post_with_err_handling(
            f"{API_ENDPOINT_URL}/products/{product_id}/artifacts/generate-upload-url",
            body={"filename": os.path.basename(file_path)},
            headers={"Authorization": f"Bearer {self.token}"},
        )

        if data and "detail" in data:
            err = self._handle_call_error(data["detail"])
            # fail!
            raise ValueError(err)
        return data

    def _upload_auth_order_artifact(
        self, file_path: str, order_id: int, is_numerai_direct: bool
    ) -> Dict[str, str]:
        data = utils.post_with_err_handling(
            f"{API_ENDPOINT_URL}/artifacts/generate-upload-url",
            body={
                "order_id": order_id,
                "filename": os.path.basename(file_path),
                "is_numerai_direct": is_numerai_direct,
            },
            headers={"Authorization": f"Bearer {self.token}"},
        )

        if data and "detail" in data:
            err = self._handle_call_error(data["detail"])
            # fail!
            raise ValueError(err)
        return data

    def _search_products(
        self, product_id: int = None, name: str = None, owner_criteria: bool = False
    ):
        json = {
            "term": name,
            "filters": {"user": {"in": [f"{self.user_id}"]}}
            if owner_criteria
            else None,
            "sort": "latest",
        }
        if product_id is not None:
            json["id"] = product_id
        data = utils.post_with_err_handling(
            f"{API_ENDPOINT_URL}/products/search-authenticated",
            json=json,
            headers={"Authorization": f"Bearer {self.token}"},
        )

        if data and "detail" in data:
            err = self._handle_call_error(data["detail"])
            # fail!
            raise ValueError(err)
        return data.get("data", [])

    def get_my_listings(self):
        """Get all your listings.
        Returns:
            list: List of dicts with the following structure:

                 * avatar (`str`)
                 * description (`str`)
                 * is_active (`bool`)
                 * is_ready (`bool`)
                 * expiration_round (`int`)
                 * total_num_sales (`int`)
                 * last_sale_price (`decimal.Decimal`)
                 * last_sale_price_delta (`decimal.Decimal`)
                 * featured_products (`list`)
                 * id (`int`)
                 * name (`str`)
                 * sku (`str`)
                 * category (`dict`) with the following fields:
                     * name (`str`)
                     * slug (`str`)
                     * tournament (`int`)
                     * is_per_round (`bool`)
                     * is_submission (`bool`)
                     * id (`int`)
                     * items (`list`)
                     * parent (`dict`)
                 * owner (`dict`)
                 * model (`dict`)
                 * reviews (`list`)
                 * options (`list`) each with the following fields:
                     * id (`int`)
                     * is_on_platform (`bool`)
                     * third_party_url (`str`)
                     * description (`str`)
                     * quantity (`int`)
                     * price (`decimal.Decimal`)
                     * currency (`str`)
                     * wallet (`str`)
                     * chain (`str`)
                     * stake_limit (`decimal.Decimal`)
                     * mode (`str`)
                     * is_active (`bool`)
                     * coupon (`bool`)
                     * coupon_specs (`dict`)
                     * special_price (`decimal.Decimal`)
                     * applied_coupon (`str`)
                     * product_id (`int`)
        Example:
            ```python
            api = NumerBay(username="..", password="..")
            api.get_my_listings()
            ```
            ```
            [{
            "avatar":"https://example.com/example.jpg",
            "description":"Product description",
            "is_active":True,
            "is_ready":False,
            "expiration_round":None,
            "total_num_sales":0,
            "last_sale_price":None,
            "last_sale_price_delta":None,
            "featured_products":None,
            "id":108,
            "name":"mymodel",
            "sku":"numerai-predictions-mymodel",
            "category":{
                "name":"Predictions",
                "slug":"numerai-predictions",
                "tournament":8,
                "is_per_round":True,
                "is_submission":True,
                "id":3,
                "items":[..],
                "parent":{..}
            },
            "owner":{
                "id":2,
                "username":"myusername"
            },
            "model":{
                "name":"mymodel",
                "tournament":8,
                "nmr_staked":100,
                "latest_ranks":{
                    "corr":100,
                    "fnc":200,
                    "mmc":300
                },
                "latest_reps":{
                    "corr":0.01,
                    "fnc":0.01,
                    "mmc":0.01
                },
                "latest_returns":{
                    "oneDay":-5.120798695681796,
                    "oneYear":None,
                    "threeMonths":-5.915974438808858
                },
                "start_date":"2020-10-25T11:08:30"
            },
            "reviews":[...],
            "options":[{
                "id":6,
                "is_on_platform":True,
                "third_party_url":None,
                "description":None,
                "quantity":1,
                "price":1,
                "currency":"NMR",
                "wallet":None,
                "chain":None,
                "stake_limit":None,
                "mode":"file",
                "is_active":True,
                "coupon":None,
                "coupon_specs":None,
                "special_price":None,
                "applied_coupon":None,
                "product_id":108
            }]
        }, ...]
        ```
        """
        return self._search_products(owner_criteria=True)

    def _resolve_product(self, product_id: int = None, product_full_name: str = None):
        if isinstance(product_id, int):
            products = self._search_products(product_id=product_id)
            if len(products) > 0:
                return products[0]
        if product_full_name is None:
            return None
        products = self._search_products(name=product_full_name.split("-")[-1])
        for product in products:
            if (
                isinstance(product_full_name, str)
                and product["sku"] == product_full_name
            ):
                return product
        return None

    def _resolve_order(
        self,
        product_id: int = None,
        order_id: int = None,
        artifact_id: Union[int, str] = None,
    ):
        my_orders = self.get_my_orders()
        for order in my_orders:
            if order["state"] != "confirmed":
                continue
            if isinstance(artifact_id, str):
                for artifact in order["artifacts"]:
                    if artifact["id"] == artifact_id:
                        return order
            else:
                if order["id"] == order_id or order["product"]["id"] == product_id:
                    return order
        return None

    def _resolve_artifact_id(
        self,
        product_id: int = None,
        order: Dict = None,
        artifact_id: Union[int, str] = None,
    ):
        # if product_id is None:
        #     return None
        if isinstance(artifact_id, (int, str)):
            return artifact_id

        if (
            order is not None
            and isinstance(order, dict)
            and order.get("buyer_public_key", None) is not None
        ):
            # resolve encrypted artifact (active and downloadable)
            artifacts = [
                artifact
                for artifact in order.get("artifacts", [])
                if artifact["state"] == "active"
                and not artifact.get("is_numerai_direct", False)
            ]
            if len(artifacts) > 0:
                last_artifact = artifacts[-1]
                return last_artifact["id"]
        else:
            # resolve unencrypted artifact
            # list product artifacts
            data = utils.get_with_err_handling(
                f"{API_ENDPOINT_URL}/products/{product_id}/artifacts",
                headers={"Authorization": f"Bearer {self.token}"},
            )

            if data and "detail" in data:
                err = self._handle_call_error(data["detail"])
                # fail!
                raise ValueError(err)

            artifacts = data.get("data", [])
            if len(artifacts) > 0:
                last_artifact = artifacts[-1]
                return last_artifact["id"]
        return None

    def _resolve_artifact_name(self, product_id: int, artifact_id: Union[int, str]):
        if isinstance(artifact_id, int):
            # resolve unencrypted artifact
            if product_id is None:
                return None

            # list product artifacts
            data = utils.get_with_err_handling(
                f"{API_ENDPOINT_URL}/products/{product_id}/artifacts",
                headers={"Authorization": f"Bearer {self.token}"},
            )

            if data and "detail" in data:
                err = self._handle_call_error(data["detail"])
                # fail!
                raise ValueError(err)

            for artifact in data.get("data", []):
                if artifact["id"] == artifact_id:
                    return artifact["object_name"]
        else:
            # resolve encrypted artifact
            data = utils.get_with_err_handling(
                f"{API_ENDPOINT_URL}/artifacts/{artifact_id}",
                headers={"Authorization": f"Bearer {self.token}"},
            )

            if data and "detail" in data:
                err = self._handle_call_error(data["detail"])
                # fail!
                raise ValueError(err)
            return data["object_name"]
        return None

    def _upload_encrypted_artifact(
        self,
        file_path: str = "predictions.csv",
        order_id: int = None,
        is_numerai_direct: bool = False,
        buyer_public_key: str = None,
        df: pd.DataFrame = None,
    ):
        # write the pandas DataFrame as a binary buffer if provided
        buffer_csv = None

        if df is not None:
            buffer_csv = BytesIO(df.to_csv(index=False).encode())
            buffer_csv.name = file_path

        upload_auth = self._upload_auth_order_artifact(
            file_path, order_id, is_numerai_direct
        )

        headers = {"Content-type": "application/octet-stream"}
        with open(file_path, "rb") if df is None else buffer_csv as file:
            # encrypt file
            if is_numerai_direct:
                encrypted_file = file
            else:
                encrypted_file = utils.encrypt_file_object(file, key=buyer_public_key)
            requests.put(upload_auth["url"], data=encrypted_file, headers=headers)

        artifact_id = upload_auth["id"]
        data = utils.post_with_err_handling(
            f"{API_ENDPOINT_URL}/artifacts/{artifact_id}/validate-upload",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        return data

    def _upload_unencrypted_artifact(
        self,
        file_path: str = "predictions.csv",
        product_id: int = None,
        df: pd.DataFrame = None,
    ):
        # write the pandas DataFrame as a binary buffer if provided
        buffer_csv = None

        if df is not None:
            buffer_csv = BytesIO(df.to_csv(index=False).encode())
            buffer_csv.name = file_path

        upload_auth = self._upload_auth(file_path, product_id)

        headers = {"Content-type": "application/octet-stream"}
        with open(file_path, "rb") if df is None else buffer_csv as file:
            requests.put(upload_auth["url"], data=file, headers=headers)

        artifact_id = upload_auth["id"]
        data = utils.post_with_err_handling(
            f"{API_ENDPOINT_URL}/products/{product_id}/artifacts/{artifact_id}/validate-upload",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        return data

    def upload_artifact(
        self,
        file_path: str = "predictions.csv",
        product_id: int = None,
        product_full_name: str = None,
        order_id: int = None,
        df: pd.DataFrame = None,
    ) -> Union[List, Dict]:
        """Upload artifact from file.
        Args:
            file_path (str): file that will get uploaded
            product_id (int): NumerBay product ID
            product_full_name (str): NumerBay product full name (e.g. numerai-predictions-numerbay),
                used for resolving product_id if product_id is not provided
            order_id (int, optional): NumerBay order ID,
                used for encrypted per-order artifact upload
            df (pandas.DataFrame): pandas DataFrame to upload, if function is
                given df and file_path, df will be uploaded.
        Returns:
            list or dict: list of artifacts uploaded (for encrypted listing)
                or the artifact uploaded (for unencrypted listing)
        Example:
            ```python
            api = NumerBay(username="..", password="..")
            product_full_name = "numerai-predictions-numerbay"
            api.upload_predictions("predictions.csv", product_full_name=product_full_name)
            # upload from pandas DataFrame directly:
            api.upload_predictions(df=predictions_df, product_full_name=product_full_name)
            ```
        """
        self.logger.info("uploading artifact...")

        # resolve product ID
        product = self._resolve_product(
            product_id=product_id, product_full_name=product_full_name
        )
        if product is None:
            raise ValueError("A valid product ID is required")
        product_id = product["id"]
        use_encryption = product.get("use_encryption", False)

        orders_to_upload = []
        has_unencrypted_sale = not use_encryption
        if use_encryption:
            active_sales = self.get_my_sales(active_only=True)
            for sale in active_sales:
                if order_id is not None and order_id != sale["id"]:
                    continue
                if sale["product"]["id"] == product_id:
                    if sale.get("buyer_public_key", None):
                        # upload encrypted artifact
                        orders_to_upload.append(sale)
                    else:
                        has_unencrypted_sale = True

        uploaded_artifacts = []
        for order in orders_to_upload:
            self.logger.info(
                f"uploading encrypted artifact for order [{order['id']}] "
                f"for [{order['buyer']['username']}]..."
            )

            # direct submission to Numerai
            if order.get("submit_model_id", None):
                data = self._upload_encrypted_artifact(
                    file_path=file_path,
                    order_id=order["id"],
                    is_numerai_direct=True,
                    buyer_public_key=order["buyer_public_key"],
                    df=df,
                )
                uploaded_artifacts.append(data)

            # upload encrypted
            if order["mode"] == "file":
                data = self._upload_encrypted_artifact(
                    file_path=file_path,
                    order_id=order["id"],
                    buyer_public_key=order["buyer_public_key"],
                    df=df,
                )
                uploaded_artifacts.append(data)

        if use_encryption and not has_unencrypted_sale:
            return uploaded_artifacts

        # upload unencrypted
        data = self._upload_unencrypted_artifact(
            file_path=file_path,
            product_id=product_id,
            df=df,
        )
        uploaded_artifacts.append(data)

        if use_encryption:
            return uploaded_artifacts
        return uploaded_artifacts[0]

    # pylint: disable=R0913
    def download_artifact(
        self,
        filename: str = None,
        dest_path: str = None,
        product_id: int = None,
        product_full_name: str = None,
        order_id: int = None,
        artifact_id: Union[int, str] = None,
        key_path: str = None,
        key_base64: str = None,
    ) -> None:
        """Download artifact file.
        Args:
            filename (str): filename to store as
            dest_path (str, optional): complate path where the file should be
                stored, defaults to the same name as the source file
            product_id (int, optional): NumerBay product ID
            product_full_name (str, optional): NumerBay product full name
                (e.g. numerai-predictions-numerbay),
                used for resolving product_id if product_id is not provided
            order_id (int, optional): NumerBay order ID,
                used for encrypted per-order artifact download
            artifact_id (str or int, optional): Artifact ID for the file to download,
                defaults to the first artifact for your active order for the product
            key_path (int, optional): path to buyer's exported NumerBay key file
            key_base64 (int, optional): buyer's NumerBay private key base64 string (used for tests)
        Example:
            ```python
            api = NumerBay(username="..", password="..")
            api.download_artifact("predictions.csv", product_id=2, artifact_id=744)
            ```
        """
        # resolve product ID
        product = self._resolve_product(
            product_id=product_id, product_full_name=product_full_name
        )
        if product is None and order_id is None and artifact_id is None:
            raise ValueError(
                "Failed to resolve artifact: "
                "make sure you have an active order for this product, "
                "and an active artifact is available for download"
            )
        product_id = product["id"] if product else None

        # resolve order ID
        order = self._resolve_order(
            product_id=product_id, order_id=order_id, artifact_id=artifact_id
        )
        if order is None and artifact_id is None:
            raise ValueError(
                "Failed to resolve artifact: "
                "make sure you have an active order for this product, "
                "and an active artifact is available for download"
            )
        if order is not None and product_id is None:
            product_id = order["product"]["id"]

        # resolve artifact ID
        # print(f"product_id: {product_id}, artifact_id: {artifact_id}, order: {order}")
        artifact_id = self._resolve_artifact_id(
            product_id=product_id, order=order, artifact_id=artifact_id
        )
        # print(f"resolved artifact_id: {artifact_id}")
        if artifact_id is None:
            raise ValueError(
                "Failed to resolve artifact: "
                "make sure you have an active order for this product, "
                "and an active artifact is available for download"
            )

        if dest_path is None:
            if filename is None:
                # resolve filename
                filename = self._resolve_artifact_name(
                    product_id=product_id, artifact_id=artifact_id
                )
            dest_path = filename

        if isinstance(artifact_id, int):
            # get url for unencrypted artifact
            file_url = utils.get_with_err_handling(
                f"{API_ENDPOINT_URL}/products/{product_id}/artifacts/{artifact_id}"
                f"/generate-download-url",
                headers={"Authorization": f"Bearer {self.token}"},
            )
        else:
            # get url for encrypted artifact
            file_url = utils.get_with_err_handling(
                f"{API_ENDPOINT_URL}/artifacts/{artifact_id}/generate-download-url",
                headers={"Authorization": f"Bearer {self.token}"},
            )

        if isinstance(file_url, dict) and "detail" in file_url:
            err = self._handle_call_error(file_url["detail"])
            # fail!
            raise ValueError(err)

        utils.download_file(file_url, dest_path, self.show_progress_bars)

        if isinstance(artifact_id, str):
            utils.decrypt_file(dest_path, key_path=key_path, key_base64=key_base64)
