from json import dumps
from json import loads
from sqlite3 import Connection
from sqlite3 import DatabaseError
from sqlite3 import OperationalError
from typing import Callable

from packaging.version import InvalidVersion
from packaging.version import Version

from acacore.__version__ import __version__

__all__ = [
    "upgrade",
    "is_latest",
]


# noinspection SqlResolve
def get_db_version(conn: Connection) -> Version | None:
    try:
        cur = conn.execute("select VALUE from Metadata where KEY like 'version'").fetchone()
        return Version(loads(cur[0])) if cur else None
    except (OperationalError, ValueError, InvalidVersion):
        return None


def set_db_version(conn: Connection, version: Version) -> Version:
    conn.execute("insert or replace into Metadata (KEY, VALUE) values (?, ?)", ("version", dumps(str(version))))
    conn.commit()
    return version


# noinspection SqlResolve
def upgrade_4to4_1(con: Connection) -> Version:
    con.execute("""
    create table files_master_tmp
    (
        uuid              text    not null,
        checksum          text    not null,
        relative_path     text    not null,
        is_binary         boolean not null,
        size              integer not null,
        puid              text,
        signature         text,
        warning           text,
        original_uuid     text,
        convert_access    text,
        convert_statutory text,
        processed         integer not null,
        primary key (relative_path)
    )
    """)

    con.execute("insert or ignore into files_master_tmp select * from files_master")
    con.execute("update files_master_tmp set processed = 4 where processed != 0")

    con.executemany(
        "update files_master_tmp set processed = processed + 1 where uuid = ? and processed != 0",
        ([uuid] for [uuid] in con.execute("select original_uuid from files_access")),
    )

    con.executemany(
        "update files_master_tmp set processed = processed + 2 where uuid = ? and processed != 0",
        ([uuid] for [uuid] in con.execute("select original_uuid from files_statutory")),
    )

    con.execute("update files_master_tmp set processed = processed - 4 where processed != 0")

    con.execute("drop table files_master")
    con.execute("alter table files_master_tmp rename to files_master")
    con.execute("vacuum")

    con.commit()

    return set_db_version(con, Version("4.1.0"))


def get_upgrade_function(current_version: Version, latest_version: Version) -> Callable[[Connection], Version]:
    if current_version < Version("4.1.0"):
        return upgrade_4to4_1
    elif current_version < latest_version:
        return lambda c: set_db_version(c, Version(__version__))
    else:
        return lambda _: latest_version


# noinspection SqlResolve
def is_latest(connection: Connection, *, raise_on_difference: bool = False) -> bool:
    """
    Check if a database is using the latest version of acacore.

    :param connection: A ``Connection`` object to the database.
    :param raise_on_difference: Set to ``True`` to raise a ``DatabaseError`` exception when the database version is
        lower than the module's
    :raises DatabaseError: If the database is not initialised, or if it is using a newer version that the
        acacore library, or ``raise_on_difference`` is set to ``True`` and the database is not up-to-date.
    :return: True if the database is using the latest version, False otherwise.
    """
    current_version: Version | None = get_db_version(connection)
    latest_version: Version = Version(__version__)

    if not current_version:
        raise DatabaseError("Database does not contain version information")
    if current_version > latest_version:
        raise DatabaseError(f"Database version is greater than latest version: {current_version} > {latest_version}")
    if current_version < latest_version and raise_on_difference:
        raise DatabaseError(f"Database version is lower than latest version: {current_version} < {latest_version}")

    return current_version == latest_version


def upgrade(connection: Connection):
    """
    Upgrade a database to the latest version of acacore.

    :param connection: A ``Connection`` object to the database.
    """
    if is_latest(connection):
        return

    current_version: Version = get_db_version(connection)
    latest_version: Version = Version(__version__)

    while current_version < latest_version:
        update_function = get_upgrade_function(current_version, latest_version)
        current_version = update_function(connection)
