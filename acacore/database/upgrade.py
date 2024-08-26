from json import dumps
from json import loads
from sqlite3 import Connection
from sqlite3 import DatabaseError
from typing import Any
from typing import Callable

from packaging.version import Version

from acacore.__version__ import __version__

__all__ = [
    "upgrade",
    "is_latest",
]


from .files_db import FileDB


def get_db_version(conn: Connection) -> Version | None:
    if res := conn.execute("select VALUE from Metadata where KEY like 'version'").fetchone():
        return Version(res[0])
    return None


def set_db_version(conn: Connection, version: Version) -> Version:
    conn.execute("insert or replace into Metadata (KEY, VALUE) values (?, ?)", ("version", str(version)))
    conn.commit()
    return version


# noinspection SqlResolve
def upgrade_1to2(conn: Connection) -> Version:
    def convert_action_data(data: dict[str, Any]) -> dict[str, Any]:
        # Rename "replace" action to "template"
        data["template"] = data.get("replace")
        data["replace"] = None
        # Remove None and empty elements (default values)
        return {k: v for k, v in data.items() if v}

    # Add "lock" column if not already present
    if not conn.execute("select 1 from pragma_table_info('Files') where name = 'lock'").fetchone():
        conn.execute("alter table Files add column lock boolean")
        # noinspection SqlWithoutWhere
        conn.execute("update Files set lock = false")
    # Rename "replace" action to "template"
    conn.execute("update Files set action = 'template' where action = 'replace'")
    # Ensure action_data is always a readable JSON
    conn.execute("update Files set action_data = '{}' where action_data is null or action_data = ''")

    # Reset _IdentificationWarnings view
    conn.execute("drop view if exists _IdentificationWarnings")
    conn.execute(
        "CREATE VIEW _IdentificationWarnings AS"
        ' SELECT * FROM Files WHERE "Files".warning is not null or "Files".puid is NULL;'
    )

    conn.executemany(
        "update Files set action_data = ? where uuid = ?",
        (
            (dumps(convert_action_data(loads(action_data_raw))), uuid)
            for uuid, action_data_raw in conn.execute("select uuid, action_data from files where action_data != '{}'")
        ),
    )

    conn.commit()

    return set_db_version(conn, Version("2.0.0"))


# noinspection SqlResolve
def upgrade_2to2_0_2(conn: Connection) -> Version:
    conn.execute("drop view if exists _IdentificationWarnings")
    conn.execute(
        "CREATE VIEW _IdentificationWarnings AS"
        ' SELECT * FROM Files WHERE "Files".warning is not null or "Files".puid is NULL;'
    )
    conn.commit()
    return set_db_version(conn, Version("2.0.2"))


# noinspection SqlResolve
def upgrade_2_0_2to3(conn: Connection) -> Version:
    def convert_action_data(data: dict):
        new_data: dict[str, Any] = {}

        if rename := data.get("rename"):
            new_data["rename"] = rename

        if reidentify := data.get("reidentify"):
            new_data["reidentify"] = {"reason": reidentify["reason"]}
            if reidentify.get("onfail"):
                new_data["reidentify"]["on_fail"] = "action"

        if convert := data.get("convert"):
            new_data["convert"] = {"tool": convert[0]["converter"], "outputs": convert[0]["outputs"]}

        if extract := data.get("extract"):
            new_data["extract"] = {"tool": extract["tool"]}
            if extension := extract.get("extension"):
                new_data["extract"]["extension"] = extension

        if manual := data.get("manual"):
            new_data["manual"] = manual

        if (ignore := data.get("ignore")) and ignore.get("reason"):
            new_data["ignore"] = {"template": "not-preservable", "reason": ignore.get("reason", "")}
        elif template := data.get("template"):
            new_data["ignore"] = {"template": template["template"]}
            if template_text := template.get("template_text"):
                new_data["ignore"]["reason"] = template_text

        return new_data

    # Add "parent" column if not already present
    if not conn.execute("select 1 from pragma_table_info('Files') where name = 'parent'").fetchone():
        conn.execute("alter table Files add column parent text")
        # noinspection SqlWithoutWhere
        conn.execute("update Files set parent = null")
    conn.execute("update Files set action = 'ignore' where action = 'template'")

    # Reset _IdentificationWarnings view
    conn.execute("drop view if exists _IdentificationWarnings")
    conn.execute(
        "CREATE VIEW _IdentificationWarnings AS"
        ' SELECT * FROM Files WHERE "Files".warning is not null or "Files".puid is NULL;'
    )

    conn.executemany(
        "update Files set action_data = ? where uuid = ?",
        (
            (dumps(convert_action_data(loads(action_data_raw))), uuid)
            for uuid, action_data_raw in conn.execute("select uuid, action_data from Files where action_data != '{}'")
        ),
    )

    conn.commit()

    return set_db_version(conn, Version("3.0.0"))


# noinspection SqlResolve
def upgrade_3to3_0_2(conn: Connection) -> Version:
    conn.execute("update Files set action = 'ignore' where action = 'template'")
    conn.execute("drop view if exists _IdentificationWarnings")
    conn.execute(
        "CREATE VIEW _IdentificationWarnings AS"
        ' SELECT * FROM Files WHERE ("Files".warning is not null or "Files".puid is null) and "Files".size != 0'
    )
    conn.commit()
    return set_db_version(conn, Version("3.0.2"))


def upgrade_3_0_2to3_0_6(conn: Connection) -> Version:
    conn.execute("update Files set action = 'ignore' where action = 'template'")
    conn.commit()
    return set_db_version(conn, Version("3.0.6"))


def upgrade_3_0_6to3_0_7(conn: Connection) -> Version:
    def convert_action_data(data: dict) -> dict | None:
        if (reidentify := data.get("reidentify")) and reidentify.get("on_fail"):
            data["reidentify"]["on_fail"] = "action"
            return data
        else:
            return None

    conn.executemany(
        "update Files set action_data = ? where uuid = ?",
        (
            (dumps(data), uuid)
            for uuid, action_data_raw in conn.execute("select uuid, action_data from Files where action_data != '{}'")
            if (data := convert_action_data(loads(action_data_raw)))
        ),
    )

    conn.commit()
    return set_db_version(conn, Version("3.0.7"))


def get_upgrade_function(current_version: Version, latest_version: Version) -> Callable[[Connection], Version]:
    if current_version < Version("2.0.0"):
        return upgrade_1to2
    elif current_version < Version("2.0.2"):
        return upgrade_2to2_0_2
    elif current_version < Version("3.0.0"):
        return upgrade_2_0_2to3
    elif current_version < Version("3.0.2"):
        return upgrade_3to3_0_2
    elif current_version < Version("3.0.6"):
        return upgrade_3_0_2to3_0_6
    elif current_version < Version("3.0.7"):
        return upgrade_3_0_6to3_0_7
    elif current_version < latest_version:
        return lambda c: set_db_version(c, Version(__version__))
    else:
        return lambda _: latest_version


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

    current_version: Version | None = get_db_version(db)
    latest_version: Version = Version(__version__)

    if not current_version:
        raise DatabaseError("Database does not contain version information")
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
