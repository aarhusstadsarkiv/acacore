from collections.abc import Callable
from enum import Enum
from re import compile as re_compile
from typing import Any
from typing import TypeVar

from pydantic import BaseModel

M = TypeVar("M", bound=BaseModel)
FC = TypeVar("FC", bound=Callable[..., Any])


class QueryOperation(Enum):
    IS = 1
    ISNOT = 2
    LIKE = 3
    LIKENOT = 4
    IN = 5
    INNOT = 6
    EQ = 7
    NE = 8
    GT = 9
    GE = 10
    LT = 11
    LE = 12

    def negate(self) -> "QueryOperation":
        return {
            self.IS: self.ISNOT,
            self.ISNOT: self.IS,
            self.LIKE: self.LIKENOT,
            self.LIKENOT: self.LIKE,
            self.IN: self.INNOT,
            self.INNOT: self.IN,
            self.EQ: self.NE,
            self.NE: self.EQ,
            self.GT: self.LE,
            self.GE: self.LT,
            self.LT: self.GE,
            self.LE: self.GT,
        }.get(self, self)


QueryValue = str | bool | list[str] | None
QueryToken = tuple[str, QueryValue, QueryOperation]  # field name, value(s), operation

token_quotes = re_compile(r'(?<!\\)"((?:[^"]|(?<=\\)")*)"')
# noinspection RegExpUnnecessaryNonCapturingGroup
token_expr = re_compile(r"(?:\x00([^\x00]+)\x00|(?<!\\)\s+)")
jsonop_expr = re_compile(r"->>?(\d+|('?)\w+\2)")
jsonops_expr = re_compile(r"^(->>?(\d+|('?)\w+\3))+$")


def tokens_to_where(query: list[QueryToken]) -> tuple[str, list[QueryValue]]:
    query_fields: dict[str, list[QueryToken]] = {}
    where: list[str] = []
    parameters: list[QueryValue] = []

    for field, value, operation in query:
        query_fields[field] = [*query_fields.get(field, []), (field, value, operation)]

    for field, values in query_fields.items():
        where_field: list[str] = []

        for _, value, op in values:
            match (value, op):
                case None, QueryOperation.IS:
                    where_field.append(f"{field} is null")
                case None, QueryOperation.ISNOT:
                    where_field.append(f"{field} is not null")
                case True, QueryOperation.IS:
                    where_field.append(f"{field} is true")
                case True, QueryOperation.ISNOT:
                    where_field.append(f"{field} is not true")
                case False, QueryOperation.IS:
                    where_field.append(f"{field} is false")
                case False, QueryOperation.ISNOT:
                    where_field.append(f"{field} is not false")
                case _, QueryOperation.LIKE:
                    where_field.append(f"{field} like ?")
                    parameters.append(value)
                case _, QueryOperation.LIKENOT:
                    where_field.append(f"{field} not like ?")
                    parameters.append(value)
                case _, QueryOperation.IN if isinstance(value, list):
                    where_field.append(f"{field} in ({','.join(['?'] * len(value))})")
                    parameters.extend(value)
                case _, QueryOperation.INNOT if isinstance(value, list):
                    where_field.append(f"{field} not in ({','.join(['?'] * len(value))})")
                    parameters.extend(value)
                case _, QueryOperation.IN if isinstance(value, str):
                    where_field.append(f"instr({field}, ?) != 0")
                    parameters.append(value)
                case _, QueryOperation.INNOT if isinstance(value, str):
                    where_field.append(f"instr({field}, ?) = 0")
                    parameters.append(value)
                case _, QueryOperation.EQ:
                    where_field.append(f"{field} = ?")
                    parameters.append(value)
                case _, QueryOperation.NE:
                    where_field.append(f"{field} != ?")
                    parameters.append(value)
                case _, QueryOperation.GT:
                    where_field.append(f"{field} > ?")
                    parameters.append(value)
                case _, QueryOperation.GE:
                    where_field.append(f"{field} >= ?")
                    parameters.append(value)
                case _, QueryOperation.LT:
                    where_field.append(f"{field} < ?")
                    parameters.append(value)
                case _, QueryOperation.LE:
                    where_field.append(f"{field} <= ?")
                    parameters.append(value)

        if where_field:
            where.append(f"({' or '.join(where_field)})")

    return " and ".join(where), parameters


def tokenizer(
    query_string: str,
    default_field: str,
    allowed_fields: list[str] | None = None,
    json_fields: list[str] | None = None,
) -> list[QueryToken]:
    query_string = token_quotes.sub(r"\0\1\0", query_string)
    tokens: list[str] = [t for t in token_expr.split(query_string) if t]
    field: str = default_field
    op: QueryOperation = QueryOperation.EQ
    neg: bool = False
    from_file: bool = False

    query_tokens: list[QueryToken] = []

    for token in tokens:
        if token == "@null":
            query_tokens.append((field, None, QueryOperation.IS.negate() if neg else QueryOperation.IS))
        elif token == "@notnull":
            query_tokens.append((field, None, QueryOperation.ISNOT.negate() if neg else QueryOperation.ISNOT))
        elif token == "@true":
            query_tokens.append((field, True, QueryOperation.IS.negate() if neg else QueryOperation.IS))
        elif token == "@false":
            query_tokens.append((field, False, QueryOperation.IS.negate() if neg else QueryOperation.IS))
        elif token == "@not":
            neg = True
        elif token == "@like":
            op = QueryOperation.LIKE
        elif token == "@gt":
            op = QueryOperation.GT
        elif token == "@ge":
            op = QueryOperation.GE
        elif token == "@lt":
            op = QueryOperation.LT
        elif token == "@le":
            op = QueryOperation.LE
        elif token == "@file":
            from_file = True
        elif token.startswith("@"):
            field, _, json_operation = token.removeprefix("@").partition("->")
            if allowed_fields and field not in allowed_fields:
                raise ValueError(f"Invalid field name {field}")
            if json_operation:
                if json_fields and field not in json_fields:
                    raise ValueError(f"Invalid JSON field name {field}")
                json_operation = f"->{json_operation}"
                if not jsonops_expr.match(json_operation):
                    raise ValueError(f"Invalid JSON operators {json_operation}")
                json_operators = [
                    o if o.isdigit() else f"'{o.strip("'")}'" for o, _ in jsonop_expr.findall(json_operation)
                ]
                field = f"{field}->{'->>'.join(json_operators)}"
            op = QueryOperation.EQ
            neg = False
            from_file = False
        elif from_file:
            with open(token) as fh:
                query_tokens.append(
                    (
                        field,
                        [line for l in fh.readlines() if (line := l.rstrip("\r\n"))],
                        QueryOperation.IN.negate() if neg else QueryOperation.IN,
                    )
                )
        else:
            query_tokens.append((field, token, op.negate() if neg else op))

    return query_tokens
