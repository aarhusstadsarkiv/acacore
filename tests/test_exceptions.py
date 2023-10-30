import pytest

from acacore.exceptions.base import ACAException
from acacore.exceptions.files import FileCollectionError
from acacore.exceptions.files import FileParseError
from acacore.exceptions.files import IdentificationError


def test_subclasses():
    with pytest.raises(ACAException):
        raise ACAException

    with pytest.raises(ACAException):
        raise IdentificationError

    with pytest.raises(ACAException):
        raise FileCollectionError

    with pytest.raises(ACAException):
        raise FileParseError
