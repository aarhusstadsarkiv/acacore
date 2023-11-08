from acacore.reference_files import get_actions
from acacore.reference_files import get_custom_signatures


def test_actions():
    assert get_actions()


def test_custom_signatures():
    assert get_custom_signatures()
