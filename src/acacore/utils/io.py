from math import floor
from math import log2
from math import log10

_exponents: list[str] = ["", "K", "M", "G", "T", "P", "E", "Z", "Y"]


def size_fmt(size: float, *, binary: bool = False, unit: str = "B") -> str:
    """
    Formats a number in SI notation.

    :param size: The number to format.
    :param binary: Whether to use binary (base 2) or decimal units.
    :param unit: The unit to use for the number, defaults to "B" for bytes.
    :return: SI-formatted number.
    """
    exponent: int
    mantissa: int

    if size == 0:
        exponent = 0
        mantissa = 1
    elif binary:
        exponent = floor(log2(abs(size)) / 10)
        mantissa = 2 ** (10 * exponent)
    else:
        exponent = floor(log10(abs(size)) / 3)
        mantissa = 10 ** (exponent * 3)

    if exponent == 0:
        return f"{size}{unit}"
    else:
        exponent_str: str = _exponents[exponent] if exponent < len(_exponents) else _exponents[-1]
        return f"{size / mantissa:.1f}{exponent_str}{'i' if binary else ''}{unit}"
