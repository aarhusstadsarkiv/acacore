from urllib.error import HTTPError

import pytest

import acacore.reference_files as reference_files


def test_actions():
    assert reference_files.get_actions()
    with pytest.raises(HTTPError) as error:
        reference_files.get.actions_file = f"wrong/path/{reference_files.get.actions_file}"
        reference_files.get_actions()
    assert error.value.code == 404


def test_custom_signatures():
    assert reference_files.get_custom_signatures()
    with pytest.raises(HTTPError) as error:
        reference_files.get.custom_signatures_file = f"wrong/path/{reference_files.get.actions_file}"
        reference_files.get_custom_signatures()
    assert error.value.code == 404
