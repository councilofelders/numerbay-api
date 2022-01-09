""" NumerBay Python API"""

import pkg_resources

try:
    __version__ = pkg_resources.get_distribution(__name__).version
except pkg_resources.DistributionNotFound:
    __version__ = "unknown"


# pylint: disable=wrong-import-position
from numerbay.numerbay import NumerBay, API_ENDPOINT_URL

# pylint: enable=wrong-import-position
