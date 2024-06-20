from functools import lru_cache
from http.client import HTTPResponse
from typing import Any
from urllib.error import HTTPError
from urllib.request import urlopen

from pydantic import TypeAdapter
from yaml import load
from yaml import Loader

from acacore.models.reference_files import Action
from acacore.models.reference_files import CustomSignature

download_url: str = "https://github.com/aarhusstadsarkiv/reference-files/releases/latest/download/"
actions_file: str = "fileformats.yml"
custom_signatures_file: str = "custom_signatures.yml"


@lru_cache
def _get_yaml(url: str) -> Any:
    response: HTTPResponse = urlopen(url)
    if response.getcode() != 200:
        raise HTTPError(url, response.getcode(), "", response.headers, response)
    return load(response.read(), Loader)


def get_actions(*, cache: bool = True) -> dict[str, Action]:
    """
    Get the actions for each of the supported PUIDs.

    The data is fetched from the repository with a cached web request.

    `Current fileformats.yml <https://github.com/aarhusstadsarkiv/reference-files/blob/main/fileformats.yml>`_

    :param cache: Use cached data if True, otherwise fetch data regardless of cache status, defaults to True.
    :raises HTTPError: If there is an issue with the request.
    :return: A dictionary with PUID keys and Action values.
    """
    url: str = f"{download_url.rstrip('/')}/{actions_file.lstrip('/')}"
    return TypeAdapter(dict[str, Action]).validate_python(_get_yaml(url) if cache else _get_yaml.__wrapped__(url))


def get_custom_signatures(*, cache: bool = True) -> list[CustomSignature]:
    """
    Gets list of custom formats with their signatures.

    The data is fetched from the repository with a cached web request.

    `Current custom_signatures.yml <https://github.com/aarhusstadsarkiv/reference-files/blob/main/custom_signatures.yml>`_

    :param cache: Use cached data if True, otherwise fetch data regardless of cache status, defaults to True.
    :raises HTTPError: If there is an issue with the request.
    :return: A list of CustomSignature objects.
    """
    url: str = f"{download_url.rstrip('/')}/{custom_signatures_file.lstrip('/')}"
    return TypeAdapter(list[CustomSignature]).validate_python(_get_yaml(url) if cache else _get_yaml.__wrapped__(url))
