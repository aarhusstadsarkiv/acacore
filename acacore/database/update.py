from sqlite3 import DatabaseError
from typing import Callable

from packaging.version import Version

from acacore.__version__ import __version__

from .files_db import FileDB

__all__ = [
    "update",
    "is_latest",
]


def get_db_version(db: FileDB) -> Version:
    return Version(db.metadata.select().version)


def set_db_version(db: FileDB, version: Version) -> Version:
    metadata = db.metadata.select()
    metadata.version = str(version)
    db.metadata.update(metadata)
    db.commit()
    return version


def get_update_function(current_version: Version, latest_version: Version) -> Callable[[FileDB], Version]:
    if current_version < Version("2.0.0"):
        return update_1to2
    elif current_version < latest_version:
        return update_last
    else:
        return lambda _: latest_version


# noinspection SqlResolve
def update_1to2(db: FileDB) -> Version:
    db.execute("alter table Files add column lock boolean default false")
    db.execute("update Files set lock = false where lock is null")
    return set_db_version(db, Version("2.0.0"))


def update_last(db: FileDB) -> Version:
    db.init()
    return set_db_version(db, Version(__version__))


def is_latest(db: FileDB) -> bool:
    """
    Check if a database is using the latest version of acacore.

    :param db: A ``FileDB`` object representing the database.
    :raises DatabaseError: If the database is not initialised, or if it is using a newer version that the
        acacore library.
    :return: True if the database is using the latest version, False otherwise.
    """
    if not db.is_initialised(check_views=False, check_indices=False):
        raise DatabaseError("Database is not initialised")

    current_version: Version = get_db_version(db)
    latest_version: Version = Version(__version__)

    if current_version > latest_version:
        raise DatabaseError(f"Database version is greater than latest version: {current_version} > {latest_version}")

    return current_version == latest_version


def update(db: FileDB):
    """
    Update a database to the latest version of acacore.

    :param db: A ``FileDB`` object representing the database.
    """
    if not db.is_initialised(check_views=False, check_indices=False):
        raise DatabaseError("Database is not initialised")

    if is_latest(db):
        return

    current_version: Version = get_db_version(db)
    latest_version: Version = Version(__version__)

    while current_version < latest_version:
        update_function = get_update_function(current_version, latest_version)
        current_version = update_function(db)
