"""NumerBay API module

Install via:
```commandline
pip install numerbay
```
"""

import os
import logging
from typing import Dict, List
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
                }
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

    def get_my_sales(self) -> List:
        """Get all your sales (including pending and expired sales).
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
                }
            }, ...]
            ```
        """
        data = utils.post_with_err_handling(
            f"{API_ENDPOINT_URL}/orders/search",
            json={"role": "seller"},
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

    def _search_products(self, name: str = None, owner_criteria: bool = False):

        data = utils.post_with_err_handling(
            f"{API_ENDPOINT_URL}/products/search-authenticated",
            json={
                "term": name,
                "filters": {"user": {"in": [f"{self.user_id}"]}}
                if owner_criteria
                else None,
                "sort": "latest",
            },
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

    def _resolve_product_id(
        self, product_id: int = None, product_full_name: str = None
    ):
        if isinstance(product_id, int):
            return product_id
        if product_full_name is None:
            return None
        products = self._search_products(name=product_full_name.split("-")[-1])
        for product in products:
            if (
                isinstance(product_full_name, str)
                and product["sku"] == product_full_name
            ):
                return product["id"]
        return None

    def _resolve_artifact_id(self, product_id: int, artifact_id: int = None):
        if product_id is None:
            return None
        if isinstance(artifact_id, int):
            return artifact_id
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
            first_artifact = artifacts[0]
            return first_artifact["id"]
        return None
        # orders = self.get_my_orders()
        # for order in orders:
        #     if order["product"]["id"] == product_id:
        #         # list product artifacts
        #         data = utils.get_with_err_handling(
        #             f"{API_ENDPOINT_URL}/products/{product_id}/artifacts",
        #             headers={"Authorization": f"Bearer {self.token}"})
        #
        #         if data and "detail" in data:
        #             err = self._handle_call_error(data['detail'])
        #             # fail!
        #             raise ValueError(err)
        #
        #         first_artifact = data.get("data", [])[0]
        #         return first_artifact["id"]
        # return None

    def _resolve_artifact_name(self, product_id: int, artifact_id: int):
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
        return None

    def upload_artifact(
        self,
        file_path: str = "predictions.csv",
        product_id: int = None,
        product_full_name: str = None,
        df: pd.DataFrame = None,
    ) -> Dict:
        """Upload artifact from file.
        Args:
            file_path (str): file that will get uploaded
            product_id (int): NumerBay product ID
            product_full_name (str): NumerBay product full name (e.g. numerai-predictions-numerbay),
                used for resolving product_id if product_id is not provided
            df (pandas.DataFrame): pandas DataFrame to upload, if function is
                given df and file_path, df will be uploaded.
        Returns:
            str: submission_id
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
        product_id = self._resolve_product_id(
            product_id=product_id, product_full_name=product_full_name
        )
        if product_id is None:
            raise ValueError("A valid product ID is required")

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

    def download_artifact(
        self,
        filename: str = None,
        dest_path: str = None,
        product_id: int = None,
        product_full_name: str = None,
        artifact_id: int = None,
    ) -> None:
        """Download artifact file.
        Args:
            filename (str): filename to store as
            dest_path (str, optional): complate path where the file should be
                stored, defaults to the same name as the source file
            product_id (int): NumerBay product ID
            product_full_name (str): NumerBay product full name (e.g. numerai-predictions-numerbay),
                used for resolving product_id if product_id is not provided
            artifact_id (str): Artifact ID for the file to download,
                defaults to the first artifact for your active order for the product
        Example:
            ```python
            api = NumerBay(username="..", password="..")
            api.download_artifact("predictions.csv", product_id=2, artifact_id=744)
            ```
        """
        # resolve product ID
        product_id = self._resolve_product_id(
            product_id=product_id, product_full_name=product_full_name
        )
        if product_id is None:
            raise ValueError("A valid product ID is required")

        # resolve artifact ID
        artifact_id = self._resolve_artifact_id(
            product_id=product_id, artifact_id=artifact_id
        )
        if artifact_id is None:
            raise ValueError(
                "Failed to resolve a valid artifact ID: "
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

        data = utils.get_with_err_handling(
            f"{API_ENDPOINT_URL}/products/{product_id}/artifacts/{artifact_id}"
            f"/generate-download-url",
            headers={"Authorization": f"Bearer {self.token}"},
        )
        print(f"data: {data}")

        if isinstance(data, dict) and "detail" in data:
            err = self._handle_call_error(data["detail"])
            # fail!
            raise ValueError(err)

        file_url = data
        utils.download_file(file_url, dest_path, self.show_progress_bars)
