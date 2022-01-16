""" collection of utility functions (from numerapi: https://github.com/uuazed/numerapi)"""

import os
import decimal
import logging
import datetime
import json
from typing import Optional, Dict

import dateutil.parser
import requests
import tqdm

logger = logging.getLogger(__name__)


def parse_datetime_string(string: str) -> Optional[datetime.datetime]:
    """try to parse string to datetime object"""
    if string is None:
        return None
    return dateutil.parser.parse(string)


def parse_float_string(string: str) -> Optional[float]:
    """try to parse string to decimal.Decimal object"""
    if string is None:
        return None
    try:
        val = decimal.Decimal(string.replace(",", ""))
    except decimal.InvalidOperation:
        val = None
    return val


def replace(dictionary: Dict, key: str, function):
    """apply a function to dict item"""
    if dictionary is not None and key in dictionary:
        dictionary[key] = function(dictionary[key])


def download_file(url: str, dest_path: str, show_progress_bars: bool = True):
    """downloads a file and shows a progress bar. allow resuming a download"""
    file_size = 0
    req = requests.get(url, stream=True)
    req.raise_for_status()

    # Total size in bytes.
    total_size = int(req.headers.get("content-length", 0))

    if os.path.exists(dest_path):
        logger.info("target file already exists")
        file_size = os.stat(dest_path).st_size  # File size in bytes
        if file_size < total_size:
            # Download incomplete
            logger.info("resuming download")
            resume_header = {"Range": f"bytes={file_size}-"}
            req = requests.get(
                url,
                headers=resume_header,
                stream=True,
                verify=False,
                allow_redirects=True,
            )
        elif file_size == total_size:
            # Download complete
            logger.info("download complete")
            return dest_path
        else:
            # Error, delete file and restart download
            logger.error("deleting file and restarting")
            os.remove(dest_path)
            file_size = 0
    else:
        # File does not exist, starting download
        logger.info("starting download")

    # write dataset to file and show progress bar
    pbar = tqdm.tqdm(
        total=total_size,
        unit="B",
        unit_scale=True,
        desc=dest_path,
        disable=not show_progress_bars,
    )
    # Update progress bar to reflect how much of the file is already downloaded
    pbar.update(file_size)
    with open(dest_path, "ab") as dest_file:
        for chunk in req.iter_content(1024):
            dest_file.write(chunk)
            pbar.update(1024)
    return dest_path


def get_with_err_handling(
    url: str, params: Dict = None, headers: Dict = None, timeout: Optional[int] = None
) -> Dict:
    """send `get` request and handle (some) errors that might occur"""
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=timeout)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as err:
        logger.error(f"Http Error: {err}, {err.response.json()}")
    except requests.exceptions.ConnectionError as err:
        logger.error(f"Error Connecting: {err}")
    except requests.exceptions.Timeout as err:
        logger.error(f"Timeout Error: {err}")
    except requests.exceptions.RequestException as err:
        logger.error(f"Oops, something went wrong: {err}")

    try:
        return resp.json()
    except UnboundLocalError:
        # `r` isn't available, probably because the try/except above failed
        pass
    except json.decoder.JSONDecodeError as err:
        logger.error(f"Did not receive a valid JSON: {err}")

    return {}


def post_with_err_handling(
    url: str,
    body: str = None,
    json: str = None,  # pylint: disable=W0621
    headers: Dict = None,
    timeout: Optional[int] = None,
) -> Dict:
    """send `post` request and handle (some) errors that might occur"""
    try:
        resp = requests.post(
            url, data=body, json=json, headers=headers, timeout=timeout
        )
        resp.raise_for_status()
    except requests.exceptions.HTTPError as err:
        logger.error(f"Http Error: {err}, {err.response.json()}")
    except requests.exceptions.ConnectionError as err:
        logger.error(f"Error Connecting: {err}")
    except requests.exceptions.Timeout as err:
        logger.error(f"Timeout Error: {err}")
    except requests.exceptions.RequestException as err:
        logger.error(f"Oops, something went wrong: {err}")

    try:
        return resp.json()
    except UnboundLocalError:
        # `r` isn't available, probably because the try/except above failed
        pass
    except json.decoder.JSONDecodeError as err:
        logger.error(f"Did not receive a valid JSON: {err}")

    return {}
