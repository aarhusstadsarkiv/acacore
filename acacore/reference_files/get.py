from functools import lru_cache
from http.client import HTTPException
from http.client import HTTPResponse
from urllib import request

from pydantic import TypeAdapter
from yaml import load
from yaml import Loader

from acacore.models.reference_files import Action
from acacore.models.reference_files import CustomSignature

download_url: str = "https://github.com/aarhusstadsarkiv/reference-files/releases/latest/download/"
actions_file: str = "fileformats.yml"
custom_signatures_file: str = "custom_signatures.json"


@lru_cache
def _get_actions(url: str) -> dict[str, Action]:
    response: HTTPResponse = request.urlopen(url)
    if response.getcode() != 200:
        raise HTTPException(response.getcode())

    return TypeAdapter(dict[str, Action]).validate_python(load(response.read(), Loader))


@lru_cache
def _get_custom_signatures(url: str) -> list[CustomSignature]:
    response: HTTPResponse = request.urlopen(url)
    if response.getcode() != 200:
        raise HTTPException(response.getcode())

    return TypeAdapter(list[CustomSignature]).validate_json(response.read())


def get_actions(use_cache: bool = True) -> dict[str, Action]:
    """
    Get the actions for each of the supported PUIDs.

    The data is fetched from the repository with a cached web request.

    Args:
        use_cache (bool): Use cached data if True, otherwise fetch data regardless of cache status.

    Returns:
        dict[str, Action]: A dictionary with PUID keys and Action values.

    See Also:
        https://github.com/aarhusstadsarkiv/reference-files/blob/main/actions.yml
    """
    return (
        _get_actions(f"{download_url.rstrip('/')}/{actions_file.lstrip('/')}")
        if use_cache
        else _get_actions.__wrapped__()
    )


def get_custom_signatures(use_cache: bool = True) -> list[CustomSignature]:
    """
    Gets list of custom formats with their signatures.

    The data is fetched from the repository with a cached web request.

    Args:
        use_cache (bool): Use cached data if True, otherwise fetch data regardless of cache status

    Returns:
        list[CustomSignature]: A list of CustomSignature objects

    See Also:
        https://github.com/aarhusstadsarkiv/reference-files/blob/main/custom_signatures.json
    """
    return (
        _get_custom_signatures(f"{download_url.rstrip('/')}/{custom_signatures_file.lstrip('/')}")
        if use_cache
        else _get_custom_signatures.__wrapped__()
    )
