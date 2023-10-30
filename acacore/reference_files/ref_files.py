import json
from functools import lru_cache
from http.client import HTTPResponse
from urllib import request

from acacore.models.reference_files import CustomSignature
from acacore.models.reference_files import ReIdentifyModel


@lru_cache
def to_re_identify() -> list[ReIdentifyModel]:
    """Gets the json file with the different formats that we wish to reidentify.

    Is kept updated on the reference-files repo. The function caches the result,
    soo multiple calls in the same run should not be an issue.
    """
    response: HTTPResponse = request.urlopen(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/main/to_reidentify.json",
    )
    if response.getcode() != 200:
        raise ConnectionError

    re_identify_map: dict[str, dict[str, str]] = json.loads(response.read())

    if re_identify_map is None:
        raise ConnectionError

    result_list: list[ReIdentifyModel] = []
    for key, values in re_identify_map.items():
        result = ReIdentifyModel(puid=key, **values)
        result_list.append(result)

    return result_list


@lru_cache
def custom_sigs() -> list[CustomSignature]:
    """Gets the json file with our own custom formats in a list.

    Is kept updated on the reference-files repo. The function caches the result,
    soo multiple calls in the same run should not be an issue.
    """
    response: HTTPResponse = request.urlopen(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/main/custom_signatures.json",
    )
    if response.getcode() != 200:
        raise ConnectionError

    custom_list: list[dict] = json.loads(response.read())

    if custom_list is None:
        raise ConnectionError

    result_list: list[CustomSignature] = []

    for values in custom_list:
        result = CustomSignature(**values)
        result_list.append(result)

    return result_list
