from collections.abc import Callable
from re import compile as re_compile
from typing import Any
from typing import TypeVar

from pydantic import BaseModel

M = TypeVar("M", bound=BaseModel)
FC = TypeVar("FC", bound=Callable[..., Any])
QueryValue = str | bool | list[str] | None
QueryTokens = list[tuple[str, QueryValue, str]]  # field name, value(s), operation

token_quotes = re_compile(r'(?<!\\)"((?:[^"]|(?<=\\)")*)"')
# noinspection RegExpUnnecessaryNonCapturingGroup
token_expr = re_compile(r"(?:\x00([^\x00]+)\x00|(?<!\\)\s+)")
jsonop_expr = re_compile(r"->>?(\d+|('?)\w+\2)")
jsonops_expr = re_compile(r"^(->>?(\d+|('?)\w+\3))+$")


def tokens_to_where(query: QueryTokens) -> tuple[str, list[QueryValue]]:
    query_fields: dict[str, list[tuple[QueryValue, str]]] = {}
    where: list[str] = []
    parameters: list[QueryValue] = []

    for field, value, operation in query:
        query_fields[field] = [*query_fields.get(field, []), (value, operation)]

    for field, values in query_fields.items():
        where_field: list[str] = []

        for value, op in values:
            match (value, op):
                case None, "is":
                    where_field.append(f"{field} is null")
                case None, "is not":
                    where_field.append(f"{field} is not null")
                case True, "is":
                    where_field.append(f"{field} is true")
                case True, "is not":
                    where_field.append(f"{field} is false")
                case False, "is":
                    where_field.append(f"{field} is false")
                case False, "is not":
                    where_field.append(f"{field} is true")
                case _, "in" if isinstance(value, list):
                    where_field.append(f"{field} in ({','.join(['?'] * len(value))})")
                    parameters.extend(value)
                case _, "not in" if isinstance(value, list):
                    where_field.append(f"{field} not in ({','.join(['?'] * len(value))})")
                    parameters.extend(value)
                case _, "in" if isinstance(value, str):
                    where_field.append(f"instr({field}, ?) != 0")
                    parameters.append(value)
                case _, "not in" if isinstance(value, str):
                    where_field.append(f"instr({field}, ?) = 0")
                    parameters.append(value)
                case _, "=":
                    where_field.append(f"{field} = ?")
                    parameters.append(value)
                case _, "!=":
                    where_field.append(f"{field} != ?")
                    parameters.append(value)
                case _, "like":
                    where_field.append(f"{field} like ?")
                    parameters.append(value)
                case _, "not like":
                    where_field.append(f"{field} not like ?")
                    parameters.append(value)

        if where_field:
            where.append(f"({' or '.join(where_field)})")

    return " and ".join(where), parameters


def tokenizer(
    query_string: str,
    default_field: str,
    allowed_fields: list[str] | None = None,
    json_fields: list[str] | None = None,
) -> QueryTokens:
    query_string = token_quotes.sub(r"\0\1\0", query_string)
    tokens: list[str] = [t for t in token_expr.split(query_string) if t]
    field: str = default_field
    like: bool = False
    neg: bool = False
    from_file: bool = False

    query_tokens: QueryTokens = []

    for token in tokens:
        if token == "@null":
            query_tokens.append((field, None, "is not" if neg else "is"))
        elif token == "@notnull":
            query_tokens.append((field, None, "is" if neg else "is not"))
        elif token == "@true":
            query_tokens.append((field, True, "is not" if neg else "is"))
        elif token == "@false":
            query_tokens.append((field, True, "is" if neg else "is not"))
        elif token == "@not":
            neg = True
        elif token == "@like":
            like = True
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
            like = False
            neg = False
            from_file = False
        elif from_file:
            with open(token) as fh:
                query_tokens.append(
                    (
                        field,
                        [line for l in fh.readlines() if (line := l.rstrip("\r\n"))],
                        "not in" if neg else "in",
                    )
                )
        else:
            query_tokens.append(
                (
                    field,
                    token,
                    "not like" if neg and like else "like" if like else "!=" if neg else "=",
                )
            )

    return query_tokens
