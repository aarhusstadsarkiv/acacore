from math import log2

binary_units = ["B", "KiB", "MiB", "GiB", "TiB"]


def size_fmt(size: float) -> str:
    """
    Formats a file size in binary multiples to a human readable string.

    Parameters
    ----------
    size: float
        The file size in bytes.

    Returns:
    -------
    str
        Human readable string representing size in binary multiples.
    """
    unit: int = int(log2(size) // 10)
    unit = unit if unit < len(binary_units) else len(binary_units) - 1
    return f"{size / (2 ** (unit * 10)):.1f} {binary_units[unit]}"
