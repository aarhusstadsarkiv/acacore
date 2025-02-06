import pytest

from acacore.exceptions.base import AcacoreError
from acacore.exceptions.files import FileCollectionError
from acacore.exceptions.files import FileParseError
from acacore.exceptions.files import IdentificationError


def test_subclasses():
    with pytest.raises(AcacoreError):
        raise AcacoreError

    with pytest.raises(AcacoreError):
        raise IdentificationError

    with pytest.raises(AcacoreError):
        raise FileCollectionError

    with pytest.raises(AcacoreError):
        raise FileParseError
