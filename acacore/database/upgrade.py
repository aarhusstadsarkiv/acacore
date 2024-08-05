from sqlite3 import DatabaseError
from typing import Callable

from packaging.version import Version

from acacore.__version__ import __version__

from .files_db import FileDB

__all__ = [
    "upgrade",
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


def get_upgrade_function(current_version: Version, latest_version: Version) -> Callable[[FileDB], Version]:
    if current_version < Version("2.0.0"):
        return upgrade_1to2
    elif current_version < latest_version:
        return upgrade_last
    else:
        return lambda _: latest_version


# noinspection SqlResolve
def upgrade_1to2(db: FileDB) -> Version:
    db.execute("alter table Files add column lock boolean default false")
    db.execute("update Files set lock = false where lock is null")
    db.execute("update Files set action = 'template' where action = 'replace'")
    for file in db.files.select():
        db.files.update(file)
    return set_db_version(db, Version("2.0.0"))


def upgrade_last(db: FileDB) -> Version:
    db.init()
    return set_db_version(db, Version(__version__))


def is_latest(db: FileDB, *, raise_on_difference: bool = False) -> bool:
    """
    Check if a database is using the latest version of acacore.

    :param db: A ``FileDB`` object representing the database.
    :param raise_on_difference: Set to ``True`` to raise a ``DatabaseError`` exception when the database version is
        lower than the module's
    :raises DatabaseError: If the database is not initialised, or if it is using a newer version that the
        acacore library, or ``raise_on_difference`` is set to ``True`` and the database is not up-to-date.
    :return: True if the database is using the latest version, False otherwise.
    """
    if not db.is_initialised(check_views=False, check_indices=False):
        raise DatabaseError("Database is not initialised")

    current_version: Version = get_db_version(db)
    latest_version: Version = Version(__version__)

    if current_version > latest_version:
        raise DatabaseError(f"Database version is greater than latest version: {current_version} > {latest_version}")
    if current_version < latest_version and raise_on_difference:
        raise DatabaseError(f"Database version is lower than latest version: {current_version} < {latest_version}")

    return current_version == latest_version


def upgrade(db: FileDB):
    """
    Upgrade a database to the latest version of acacore.

    :param db: A ``FileDB`` object representing the database.
    """
    if not db.is_initialised(check_views=False, check_indices=False):
        raise DatabaseError("Database is not initialised")

    if is_latest(db):
        return

    current_version: Version = get_db_version(db)
    latest_version: Version = Version(__version__)

    while current_version < latest_version:
        update_function = get_upgrade_function(current_version, latest_version)
        current_version = update_function(db)
