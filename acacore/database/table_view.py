from sqlite3 import Connection
from typing import Generator
from typing import Generic
from typing import Self
from typing import Type

from .column import SQLValue
from .cursor import Cursor
from .table import _M
from .table import _W
from .table import Table


class View(Generic[_M]):
    """
    A class that represents a view in an SQLite database and allows accessing rows as Pydantic models.

    :ivar database: The connection to the database.
    :ivar model: The model the view is based on.
    :ivar name: The name of the view.
    :ivar select_stmt: The select statement used to create the view.
    """

    def __init__(
        self,
        database: Connection,
        model: Type[_M],
        name: str,
        select: str,
        ignore: list[str] | None = None,
    ) -> None:
        """
        :param model: The Pydantic model to create the view for.
        :param name: The name of the view.
        :param select: The select SQL expression to use to populate the view.
        :param ignore: A list of field names to ignore from the model.
        """  # noqa: D205
        self.database: Connection = database
        self.model: Type[_M] = model
        self.name: str = name
        self.select_stmt: str = select
        self._table: Table[_M] = Table(self.database, self.model, self.name, ignore=ignore)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name!r}, {self.model.__name__})"

    def __iter__(self) -> Generator[_M, None, None]:
        yield from self.select()

    def __len__(self) -> int:
        return len(self._table)

    def __getitem__(self, where: _W | _M) -> _M | None:
        return self._table.select(where, limit=1).fetchone()

    def __contains__(self, where: _M) -> bool:
        return self._table.select(where, limit=1).cursor.fetchone() is not None

    def create_sql(self, *, exist_ok: bool = False) -> str:
        """Generate the SQL statement to create the view."""
        return f"create view {'if not exists' if exist_ok else ''} {self.name} as {self.select_stmt}"

    def create(self, *, exist_ok: bool = False) -> Self:
        """
        Create the view in the connected database.

        :param exist_ok: Whether to ignore any existing view with the same name.
        """
        self.database.execute(self.create_sql(exist_ok=exist_ok))
        return self

    def select(
        self,
        where: _W | None = None,
        params: list[SQLValue] | None = None,
        order_by: list[tuple[str, str]] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Cursor[_M]:
        """
        Select entries from the view.

        :param where: The where statement to use. This can be a string, a dictionary containing column names and
            values, or an instance of the model used by the table if primary keys have been defined.
        :param params: The parameters to use for the query, they are ignored if the ``where`` argument is anything but
            a string.
        :param order_by: A list of column names and direction ("asc", "desc") tuples to sort the results.
        :param limit: The maximum number of results to return.
        :param offset: The offset to start the results from.
        :return: A ``Cursor`` instance.
        """
        return self._table.select(where, params, order_by, limit, offset)
