import json
from functools import lru_cache
from http.client import HTTPResponse
from urllib import request

from pydantic import TypeAdapter

from acacore.models.reference_files import ConversionInstruction
from acacore.models.reference_files import CustomSignature
from acacore.models.reference_files import ExtractionInstruction
from acacore.models.reference_files import IgnoreInstruction
from acacore.models.reference_files import ManualConversionInstruction
from acacore.models.reference_files import ReIdentifyModel


@lru_cache
def _get_conversion_instructions() -> list[ConversionInstruction]:
    response: HTTPResponse = request.urlopen(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/main/to_convert.json",
    )

    if response.getcode() != 200:
        raise ConnectionError

    instructions: dict[str, dict] = json.loads(response.read())

    response = request.urlopen(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/main/to_convert_symphovert.json",
    )

    if response.getcode() != 200:
        raise ConnectionError

    instructions.update(json.loads(response.read()))

    return [ConversionInstruction(puid=puid, **value) for puid, value in instructions]


@lru_cache
def _get_manual_conversion_instructions() -> list[ManualConversionInstruction]:
    response: HTTPResponse = request.urlopen(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/main/manual_convert.json",
    )

    if response.getcode() != 200:
        raise ConnectionError

    return [ManualConversionInstruction(puid=puid, **value) for puid, value in json.loads(response.read()).items()]


@lru_cache
def _get_extraction_instructions() -> list[ExtractionInstruction]:
    response: HTTPResponse = request.urlopen(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/main/to_extract.json",
    )

    if response.getcode() != 200:
        raise ConnectionError

    return [ExtractionInstruction(puid=puid, **value) for puid, value in json.loads(response.read()).items()]


@lru_cache
def _get_ignore_instructions() -> list[IgnoreInstruction]:
    response: HTTPResponse = request.urlopen(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/main/to_ignore.json",
    )

    if response.getcode() != 200:
        raise ConnectionError

    return [IgnoreInstruction(puid=puid, **value) for puid, value in json.loads(response.read()).items()]


@lru_cache
def _get_to_re_identify() -> list[ReIdentifyModel]:
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
def _get_custom_signatures() -> list[CustomSignature]:
    response: HTTPResponse = request.urlopen(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/main/custom_signatures.json",
    )
    if response.getcode() != 200:
        raise ConnectionError

    custom_list: list[dict] = json.loads(response.read())

    if custom_list is None:
        raise ConnectionError

    return TypeAdapter(list[CustomSignature]).validate_python(custom_list)


def get_conversion_instructions(use_cache: bool = True) -> list[ConversionInstruction]:
    return _get_conversion_instructions() if use_cache else _get_conversion_instructions.__wrapped__()


def get_manual_conversion_instructions(use_cache: bool = True) -> list[ManualConversionInstruction]:
    return _get_manual_conversion_instructions() if use_cache else _get_manual_conversion_instructions.__wrapped__()


def get_extraction_instructions(use_cache: bool = True) -> list[ExtractionInstruction]:
    return _get_extraction_instructions() if use_cache else _get_extraction_instructions.__wrapped__()


def get_ignore_instructions(use_cache: bool = True) -> list[IgnoreInstruction]:
    return _get_ignore_instructions() if use_cache else _get_ignore_instructions.__wrapped__()


def get_to_re_identify(use_cache: bool = True) -> list[ReIdentifyModel]:
    """
    Gets the json file with the different formats that we wish to re-identify.

    Is kept updated on the reference-files repo. The function caches the result,
    soo multiple calls in the same run should not be an issue.
    """
    return _get_to_re_identify() if use_cache else _get_to_re_identify.__wrapped__()


def get_custom_signatures(use_cache: bool = True) -> list[CustomSignature]:
    """
    Gets the json file with our own custom formats in a list.

    Is kept updated on the reference-files repo. The function caches the result,
    soo multiple calls in the same run should not be an issue.
    """
    return _get_custom_signatures() if use_cache else _get_custom_signatures.__wrapped__()
