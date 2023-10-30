from pytest import raises

from acacore.exceptions.base import ACAException
from acacore.exceptions.files import FileCollectionError
from acacore.exceptions.files import FileParseError
from acacore.exceptions.files import IdentificationError


def test_subclasses():
    with raises(ACAException):
        raise ACAException()

    with raises(ACAException):
        raise IdentificationError()

    with raises(ACAException):
        raise FileCollectionError()

    with raises(ACAException):
        raise FileParseError()
