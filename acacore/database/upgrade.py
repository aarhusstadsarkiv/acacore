from json import dumps
from json import loads
from sqlite3 import DatabaseError
from sqlite3 import Row
from typing import Any
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
    elif current_version < Version("2.0.2"):
        return upgrade_2to2_0_2
    elif current_version < latest_version:
        return upgrade_last
    else:
        return lambda _: latest_version


# noinspection SqlResolve
def upgrade_1to2(db: FileDB) -> Version:
    if not db.execute("select 1 from pragma_table_info('Files') where name = 'lock'").fetchone():
        db.execute("alter table Files add column lock boolean")
        db.execute("update Files set lock = false")
    # Rename "replace" action to "template"
    db.execute("update Files set action = 'template' where action = 'replace'")
    # Ensure action_data is always a readable JSON
    db.execute("update Files set action_data = '{}' where action_data is null or action_data = ''")

    # Reset _IdentificationWarnings view
    db.execute("drop view if exists _IdentificationWarnings")
    db.identification_warnings.create()

    cursor = db.execute("select * from files where action_data != '{}'")
    cursor.rowfactory = Row

    for file in cursor:
        action_data: dict[str, Any] = loads(file["action_data"])
        # Rename "replace" action to "template"
        action_data["template"] = action_data.get("replace")
        # Remove None and empty lists (default values)
        action_data = {k: v for k, v in action_data.items() if v}
        db.execute("update Files set action_data = ? where uuid = ?", [dumps(action_data), file["uuid"]])

    db.commit()

    return set_db_version(db, Version("2.0.0"))


def upgrade_2to2_0_2(db: FileDB) -> Version:
    db.execute("drop view if exists _IdentificationWarnings")
    db.identification_warnings.create()
    db.commit()
    return set_db_version(db, Version("2.0.2"))


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
    if db.committed_changes != db.total_changes:
        raise DatabaseError("Database has uncommited transactions")

    if is_latest(db):
        return

    current_version: Version = get_db_version(db)
    latest_version: Version = Version(__version__)

    while current_version < latest_version:
        update_function = get_upgrade_function(current_version, latest_version)
        current_version = update_function(db)
