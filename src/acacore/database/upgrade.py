from collections.abc import Callable
from collections.abc import Iterable
from itertools import batched
from json import dumps
from json import loads
from os import PathLike
from pathlib import Path
from sqlite3 import Connection
from sqlite3 import DatabaseError
from sqlite3 import OperationalError

from packaging.version import InvalidVersion
from packaging.version import Version

from acacore.__version__ import __version__

__all__ = [
    "is_latest",
    "upgrade",
]


# noinspection SqlResolve
def get_db_version(conn: Connection) -> Version | None:
    try:
        cur = conn.execute("select VALUE from Metadata where KEY like 'version'").fetchone()
        return Version(loads(cur[0])) if cur else None
    except (OperationalError, ValueError, InvalidVersion):
        return None


def set_db_version(conn: Connection, version: Version) -> Version:
    conn.execute(
        "insert or replace into Metadata (KEY, VALUE) values (?, ?)",
        ("version", dumps(str(version))),
    )
    conn.commit()
    return version


def table_columns(con: Connection, table: str) -> list[str]:
    return [p[1].lower() for p in con.execute(f'pragma table_info("{table}")')]


# noinspection SqlResolve
def upgrade_4to4_1(con: Connection, _root: Path) -> Version:
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

    con.execute("drop view files_all")
    con.execute("drop view log_paths")
    con.execute("drop table files_master")
    con.execute("alter table files_master_tmp rename to 'files_master'")
    con.execute("""
    create view files_all as
    select uuid,
       checksum,
       relative_path,
       is_binary,
       size,
       puid,
       signature,
       warning
    from files_original
    union
    select uuid,
           checksum,
           relative_path,
           is_binary,
           size,
           puid,
           signature,
           warning
    from files_master
    union
    select uuid,
           checksum,
           relative_path,
           is_binary,
           size,
           puid,
           signature,
           warning
    from files_access
    union
    select uuid,
           checksum,
           relative_path,
           is_binary,
           size,
           puid,
           signature,
           warning
    from files_statutory
    """)
    con.execute("""
    create view log_paths as
    select coalesce(fo.relative_path, fm.relative_path, fa.relative_path, fs.relative_path) as file_relative_path, l.*
    from log l
        left join files_original  fo on l.file_type = 'original'  and fo.uuid = l.file_uuid
        left join files_master    fm on l.file_type = 'master'    and fm.uuid = l.file_uuid
        left join files_access    fa on l.file_type = 'access'    and fa.uuid = l.file_uuid
        left join files_statutory fs on l.file_type = 'statutory' and fs.uuid = l.file_uuid
    """)
    con.commit()

    con.execute("vacuum")

    return set_db_version(con, Version("4.1.0"))


def upgrade_4_1to4_1_1(con: Connection, _root: Path) -> Version:
    con.execute("drop table metadata")
    con.execute("create table metadata (key text not null, value text, primary key (key))")
    con.commit()
    return set_db_version(con, Version("4.1.1"))


def upgrade_5to5_1(con: Connection, _root: Path) -> Version:
    con.execute("alter table files_statutory add column doc_collection int")
    con.execute("alter table files_statutory add column doc_id int")
    con.commit()
    return set_db_version(con, Version("5.1.0"))


def upgrade_5_1to5_2(con: Connection, root: Path) -> Version:
    from chardet import UniversalDetector

    con.execute("drop view if exists files_all")

    if "encoding" not in table_columns(con, "files_original"):
        con.execute("alter table files_original add column encoding text")
    if "encoding" not in table_columns(con, "files_master"):
        con.execute("alter table files_master add column encoding text")
    if "encoding" not in table_columns(con, "files_access"):
        con.execute("alter table files_access add column encoding text")
    if "encoding" not in table_columns(con, "files_statutory"):
        con.execute("alter table files_statutory add column encoding text")

    def _encoding(path: str | PathLike[str]) -> dict | None:
        detector = UniversalDetector()
        with open(path, "rb") as f:
            while chunk := f.read(2**20):
                detector.feed(chunk)
        detector.close()
        return enc if (enc := detector.result).get("encoding") else None

    # noinspection SqlResolve
    for table in ("files_original", "files_master", "files_access", "files_statutory"):
        batch: Iterable[tuple[str, str]]
        for batch in batched(con.execute(f"select uuid, relative_path from {table} where is_binary is false"), 1000):
            con.executemany(
                f"update {table} set encoding = ? where uuid = ?",
                ((dumps(enc), uuid) for [uuid, path] in batch if (enc := _encoding(root.joinpath(path)))),
            )
            con.commit()

    con.commit()

    return set_db_version(con, Version("5.2.0"))


def get_upgrade_function(current_version: Version, latest_version: Version) -> Callable[[Connection, Path], Version]:
    if current_version < Version("4.1.0"):
        return upgrade_4to4_1
    elif current_version < Version("4.1.1"):
        return upgrade_4_1to4_1_1
    elif current_version < Version("5.1.0"):
        return upgrade_5to5_1
    elif current_version < Version("5.2.0"):
        return upgrade_5_1to5_2
    elif current_version < latest_version:
        return lambda c, _: set_db_version(c, Version(__version__))
    else:
        return lambda _, __: latest_version


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


def upgrade(connection: Connection, files_root: str | PathLike[str]):
    """
    Upgrade a database to the latest version of acacore.

    :param connection: A ``Connection`` object to the database.
    :param files_root: Root directory of the files.
    """
    if is_latest(connection):
        return

    current_version: Version = get_db_version(connection)
    latest_version: Version = Version(__version__)

    while current_version < latest_version:
        update_function = get_upgrade_function(current_version, latest_version)
        current_version = update_function(connection, Path(files_root))
