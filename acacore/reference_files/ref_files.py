import json
from functools import lru_cache
from http.client import HTTPResponse
from urllib import request


@lru_cache
def to_re_identify() -> dict[str, str]:
    """Gets the json file with the different formats that we wish to reidentify.

    Is kept updated on the reference-files repo. The function caches the result,
    soo multiple calls in the same run should not be an issue.
    """
    response: HTTPResponse = request.urlopen("https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/main/to_reidentify.json")
    if response.getcode() != 200:
        raise ConnectionError

    re_identify_map: dict[str, str] = json.loads(response.read())

    if re_identify_map is None:
        raise ConnectionError

    return re_identify_map


@lru_cache
def costum_sigs() -> list[dict]:
    """Gets the json file with our own costum formats in a list.

    Is kept updated on the reference-files repo. The function caches the result,
    soo multiple calls in the same run should not be an issue.
    """
    response: HTTPResponse = request.urlopen(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/main/custom_signatures.json",
    )
    if response.getcode() != 200:
        raise ConnectionError

    re_identify_map: dict[str, str] = json.loads(response.read())

    if re_identify_map is None:
        raise ConnectionError

    return re_identify_map
